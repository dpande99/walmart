import json
import logging
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed # Import as_completed
import threading
from api.models.schemas import MessageContent, AgentResponse
from core.agents_sequential import AgentFactorySequential
from core.orchestration_tools import build_m_schema_string
from config.settings import get_settings
import re # Already in your original file, good for the final selector

# Import get_data_dictionary_tables if it exists in a module, e.g. core.schema_tools
try:
    from core.schema_tools import get_data_dictionary_tables, get_all_db_objects
except ImportError:
    # Define dummy functions or handle the import error as needed
    def get_data_dictionary_tables():
        raise NotImplementedError("get_data_dictionary_tables is not implemented or imported.")

    def get_all_db_objects():
        raise NotImplementedError("get_all_db_objects is not implemented or imported.")

settings = get_settings()
logger = logging.getLogger(__name__)

class AgentManagerSequential:
    def __init__(self, api_key: str = None, model: str = None, temperature: float = None):
        self.api_key = api_key or settings.LLM_API_KEY
        self.model = model or settings.LLM_MODEL
        self.temperature = temperature if temperature is not None else settings.LLM_TEMPERATURE
        # Note: Threading might be overkill if FastAPI runs each request in its own thread, but it's safe.
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="agent_worker")
        self._lock = threading.Lock()

    def process_query(self, query: str) -> AgentResponse:
        logger.info(f"AgentManager.process_query received query: '{query}'")
        with self._lock:
            result = self._orchestrate_workflow(query)
        logger.info(f"AgentManager.process_query finished for query: '{query}'")
        return result

    def _orchestrate_workflow(self, query: str) -> AgentResponse:
        logger.info("--- XIYAN-SQL ORCHESTRATED WORKFLOW START ---")
        full_conversation_history = []
        
        try:
            # Create our toolkit of agents
            agents = AgentFactorySequential.create_agents(
                api_key=self.api_key, model=self.model, temperature=self.temperature
            )
            print("created new sequential agents")
            user_proxy = agents.pop("UserProxy")

            # === STAGE 1: SCHEMA LINKING ===
            logger.info("[Orchestrator] STAGE 1: Schema Linking")
            schema_analyst = agents["SchemaAnalyst"]
            task_1_prompt = f"User Question: '{query}'"
            chat_res_1 = user_proxy.initiate_chat(schema_analyst, message=task_1_prompt, clear_history=True, max_turns=3)
            full_conversation_history.extend(chat_res_1.chat_history)
            try:
                selected_tables = self._parse_json_list(chat_res_1, 'tables')
            except ValueError:
                logger.warning("Falling back to all tables from get_all_db_objects due to missing 'tables' key.")
                selected_tables = [f'{t["schema"]}.{t["name"]}' for t in get_all_db_objects()["tables"]]            
                logger.info(f"[Orchestrator] Selected Tables: {selected_tables}")

            column_selector = agents["ColumnSelector"]
            task_2_prompt = f"Relevant Tables: {selected_tables}\nUser Question: '{query}'"
            chat_res_2 = user_proxy.initiate_chat(column_selector, message=task_2_prompt, clear_history=True, max_turns=5)
            full_conversation_history.extend(chat_res_2.chat_history)
            selected_columns = self._parse_json_list(chat_res_2, 'columns')
            logger.info(f"[Orchestrator] Selected Columns: {selected_columns}")

            # === STAGE 2: M-SCHEMA CONSTRUCTION ===
            logger.info("[Orchestrator] STAGE 2: M-Schema Construction")
            m_schema = build_m_schema_string(tables=selected_tables, columns=selected_columns)
            logger.info(f"[Orchestrator] Constructed M-Schema:\n{m_schema}")
            full_conversation_history.append({"role": "system", "name": "Orchestrator", "content": f"M-Schema constructed:\n{m_schema}"})

            # === STAGE 3: CANDIDATE GENERATION (MODIFIED FOR PARALLELISM) ===
            logger.info("[Orchestrator] STAGE 3: Candidate Generation (Parallel)")
            sql_generator = agents["SQLGenerator"]
            candidate_queries = []
            temperatures_to_try = [0.0, 0.2, 0.4, 0.6, 0.8]  

            generation_prompt = f"""
            User Question: '{query}'

            M-Schema:
            {m_schema}
            """
            
            # Use a ThreadPoolExecutor to run generation in parallel
            with ThreadPoolExecutor(max_workers=len(temperatures_to_try)) as executor:
                # Store futures to retrieve results later
                future_to_temp = {
                    executor.submit(self._generate_single_candidate, sql_generator, generation_prompt, temp): temp
                    for temp in temperatures_to_try
                }

                for future in as_completed(future_to_temp):
                    temp = future_to_temp[future]
                    try:
                        sql_candidate = future.result()
                        if sql_candidate:
                            logger.info(f"Successfully generated candidate with temperature {temp}.")
                            candidate_queries.append(sql_candidate)
                            # Log interaction for debugging
                            full_conversation_history.append({"role": "system", "name": "Orchestrator", "content": f"Generated candidate with temp {temp}"})
                            full_conversation_history.append({"role": "user", "content": generation_prompt})
                            full_conversation_history.append({"role": "assistant", "name": "SQLGenerator", "content": sql_candidate})
                        else:
                            logger.warning(f"SQLGenerator produced an empty response for temperature {temp}.")
                    except Exception as exc:
                        logger.error(f"Candidate generation with temperature {temp} failed: {exc}", exc_info=True)

            logger.info(f"[Orchestrator] Final Generated Candidates: {candidate_queries}")
            if not candidate_queries:
                raise ValueError("SQLGenerator failed to produce any valid candidates. Halting workflow.")

            # === STAGE 4: VALIDATION & REFINEMENT ===
            # (This section remains unchanged from your original code)
            logger.info("[Orchestrator] STAGE 4: Validation & Refinement")
            sql_validator = agents["SQLValidator"]
            validated_results = []
            for i, sql in enumerate(candidate_queries):
                task_4_prompt = f"Validate, refine if necessary, and execute this query:\n```sql\n{sql}\n```"
                chat_res_4 = user_proxy.initiate_chat(sql_validator, message=task_4_prompt, clear_history=True)
                full_conversation_history.extend(chat_res_4.chat_history)
                last_response = chat_res_4.summary
                try:
                    validation_dict = json.loads(self._extract_json_from_string(last_response))
                except (json.JSONDecodeError, TypeError):
                    validation_dict = {"final_query": sql, "result": [{"error": "Failed to parse validator's JSON response."}]}
                validated_results.append(validation_dict)
            logger.info(f"[Orchestrator] Validated Results: {validated_results}")

            # 🚨 Remove duplicate responses before final selection
            validated_results = self.deduplicate_responses(validated_results)
            if self.has_duplicate_responses(validated_results):
                print("🛑 Detected repeated output (loop) after deduplication!")

            # === STAGE 5: FINAL SELECTION ===
            # (This section remains unchanged from your original code)
            logger.info("[Orchestrator] STAGE 5: Final Selection")
            final_selector = agents["FinalSelector"]
            selection_prompt = f"Original Question: '{query}'\n\n"
            for i, res in enumerate(validated_results):
                result_preview = str(res.get('result', 'No result'))[:200]
                selection_prompt += f"--- Candidate {chr(65+i)} ---\nSQL: {res.get('final_query', 'No query')}\nResult Preview: {result_preview}...\n\n"
            selection_prompt += "Which candidate is the best answer? Respond with ONLY the single character of your choice (e.g., A, B, or C)."
            
            chat_res_5 = user_proxy.initiate_chat(final_selector, message=selection_prompt, clear_history=True, max_turns=1)
            full_conversation_history.extend(chat_res_5.chat_history)

            final_choice_letter = ""
            summary_text = chat_res_5.summary.upper().strip()

            match = re.search(r'[A-Z]', summary_text)
            if match:
                final_choice_letter = match.group(0)
            else:
                logger.warning(f"FinalSelector did not provide a valid choice. Defaulting to 'A'. Summary was: '{chat_res_5.summary}'")
                final_choice_letter = "A"

            final_answer_index = ord(final_choice_letter) - 65
            if final_answer_index < 0 or final_answer_index >= len(validated_results):
                logger.warning(f"FinalSelector chose '{final_choice_letter}', which is out of bounds. Defaulting to index 0.")
                final_answer_index = 0

            final_answer_obj = validated_results[final_answer_index]
            final_answer_str = json.dumps(final_answer_obj, indent=2)
            logger.info(f"[Orchestrator] Final Choice: {final_choice_letter}. Final Answer: {final_answer_str}")

            return AgentResponse(
                conversation=[MessageContent(**msg) for msg in full_conversation_history if msg.get("role") and msg.get("content")],
                final_answer=final_answer_str
            )
        except Exception as e:
            logger.error(f"Error in orchestration workflow: {e}", exc_info=True)
            return AgentResponse(conversation=[], final_answer=f"An error occurred: {e}")

    # Helper function for parallel execution
    def _generate_single_candidate(self, agent: Any, prompt: str, temperature: float) -> str:
        """Generates a single SQL candidate query in a thread-safe way."""
        
        # Create a temporary config to avoid race conditions on the agent's main llm_config
        temp_llm_config = agent.llm_config.copy()
        temp_llm_config['temperature'] = temperature

        # Use the more direct generate_reply with the temporary config
        response_message = agent.generate_reply(
            messages=[{"role": "user", "content": prompt}],
            config=temp_llm_config # Pass the temporary config here
        )
        
        sql_candidate = ""
        if response_message and isinstance(response_message, dict):
            sql_candidate = response_message.get("content", "").strip()
        elif response_message and isinstance(response_message, str):
            sql_candidate = response_message.strip()

        # Clean up potential markdown code blocks
        if sql_candidate.startswith("```sql"):
            sql_candidate = sql_candidate[6:]
        if sql_candidate.endswith("```"):
            sql_candidate = sql_candidate[:-3]
        
        return sql_candidate.strip()

    def _extract_json_from_string(self, text: str) -> str:
        """
    Finds and returns the first valid JSON object substring in a string.
    Handles cases where the LLM might add leading/trailing text or newlines.
    """
    # Find the start of the first potential JSON object
        start_brace = text.find('{')
        if start_brace == -1:
            return "{}" # No JSON object found

        # Start from the first brace and try to decode a JSON object
        potential_json = text[start_brace:]
        decoder = json.JSONDecoder()
        try:
            # decode will find the first valid JSON object and stop
            obj, end_index = decoder.raw_decode(potential_json)
            # Return the substring that constitutes the valid JSON
            return potential_json[:end_index]
        except json.JSONDecodeError:
            # If raw_decode fails, it means there's no valid JSON object starting from the first brace.
            logger.warning(f"Could not find a valid JSON object in the text: {text}")
            return "{}"

    def _parse_json_list(self, chat_result: Any, key: str) -> List[str]:
        """Parses a JSON list from the last message of a chat result."""
        try:
            last_message = chat_result.chat_history[-1]['content']
            json_str = self._extract_json_from_string(last_message)
            data = json.loads(json_str)
            value = data.get(key, [])
            # If value is not a list or is empty, trigger fallback
            if not isinstance(value, list) or not value:
                raise ValueError(f"Key '{key}' missing or empty in agent response.")
            return value
        except (json.JSONDecodeError, KeyError, IndexError, AttributeError, ValueError) as e:
            logger.error(f"Failed to parse JSON list with key '{key}': {e}. Content was: '{last_message}'")
            raise ValueError(f"Orchestrator could not parse a required response for key: {key}")

    def close(self):
        logger.info("Shutting down AgentManager executor.")
        if hasattr(self, '_executor') and self._executor:
            self._executor.shutdown(wait=True)

    def has_duplicate_results(results):
        """
        Returns True if there are duplicate rows in the results list.
        """
        seen = set()
        for row in results:
            row_tuple = tuple(sorted(row.items()))
            if row_tuple in seen:
                return True
            seen.add(row_tuple)
        return False

    def has_duplicate_responses(self, responses):
        """
        Returns True if there are duplicate SQL/result pairs in the responses list.
        """
        seen = set()
        for resp in responses:
            key = (resp.get('final_query'), str(resp.get('result')))
            if key in seen:
                print("🛑 Detected repeated output (loop)!")
                return True
            seen.add(key)
        return False

    def deduplicate_responses(self, responses):
        """
        Remove duplicate responses based on their SQL and result.
        """
        seen = set()
        unique_responses = []
        for resp in responses:
            key = (resp.get('final_query'), str(resp.get('result')))
            if key not in seen:
                unique_responses.append(resp)
                seen.add(key)
            else:
                print("🛑 Duplicate response removed from output log!")
        return unique_responses

    def get_data_dictionary_tables(self):
        """
        Example for SchemaAnalyst agent (pseudo-code)
        """
        try:
            tables = get_data_dictionary_tables()
            # ...process tables as usual...
        except Exception:
            # Fallback: use all tables from get_all_db_objects
            tables = [f'{t["schema"]}.{t["name"]}' for t in get_all_db_objects()["tables"]]
            # Always return the required format
            return {"tables": tables}

