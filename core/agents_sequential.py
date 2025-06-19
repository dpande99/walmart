from typing import Dict
import logging
from autogen import ConversableAgent
from config.settings import get_settings
from tools.db_tools import query_database, explain_query, get_data_dictionary_tables, get_data_dictionary_columns,get_all_db_objects, get_complete_schema
from prompts.agent_prompts_sequential import (
    SCHEMA_ANALYST_PROMPT,
    COLUMN_SELECTOR_PROMPT,
    SQL_GENERATOR_PROMPT,
    SQL_VALIDATOR_PROMPT,
    FINAL_SELECTOR_PROMPT,
)

settings = get_settings()
logger = logging.getLogger(__name__)

class AgentFactorySequential:
    """Factory class for creating a toolkit of specialized agents."""

    @staticmethod
    def create_agents(api_key: str = None, model: str = None, temperature: float = None) -> Dict[str, ConversableAgent]:
        """
        Creates and configures all specialized agents for the XiYan-SQL pipeline.

        Returns:
            dict: A dictionary of configured ConversableAgent instances.
        """
        api_key = api_key or settings.LLM_API_KEY
        model = model or settings.LLM_MODEL
        temperature = temperature if temperature is not None else settings.LLM_TEMPERATURE

        llm_config = {"config_list": [{"model": model, "api_key": api_key}], "temperature": temperature}

        # --- STAGE 1 AGENTS ---
        schema_analyst = ConversableAgent(
            name="SchemaAnalyst",
            system_message=SCHEMA_ANALYST_PROMPT,
            llm_config=llm_config.copy(),
        )

        column_selector = ConversableAgent(
            name="ColumnSelector",
            system_message=COLUMN_SELECTOR_PROMPT,
            llm_config=llm_config.copy(),
        )

        # --- STAGE 3 AGENT ---
        sql_generator = ConversableAgent(
            name="SQLGenerator",
            system_message=SQL_GENERATOR_PROMPT,
            llm_config=llm_config.copy(),
        )

        # --- STAGE 4 AGENT ---
        sql_validator = ConversableAgent(
            name="SQLValidator",
            system_message=SQL_VALIDATOR_PROMPT,
            llm_config=llm_config.copy(),
        )

        # --- STAGE 5 AGENT ---
        final_selector = ConversableAgent(
            name="FinalSelector",
            system_message=FINAL_SELECTOR_PROMPT,
            llm_config=llm_config.copy(),
        )
        
        # --- The User Proxy to execute tool calls ---
        user_proxy = ConversableAgent(
            name="UserProxy",
            llm_config=False,
            is_termination_msg=lambda msg: (
        msg is not None and msg.get("content") is not None and "TERMINATE" in msg.get("content", "")
    ),
            human_input_mode="NEVER",
            code_execution_config={"work_dir": "coding", "use_docker": False}, # Required for tool execution
        )

        # --- TOOL REGISTRATION ---
        # Register tools for the agents that need them
        schema_analyst.register_for_llm(name="get_all_db_objects", description="Get a raw list of all tables, views, and materialized views from the database.")(get_all_db_objects)
        user_proxy.register_for_execution(name="get_all_db_objects")(get_all_db_objects)
        
        column_selector.register_for_llm(name="get_complete_schema", description="Get the complete technical schema (columns, types, keys) for all tables.")(get_complete_schema)
        user_proxy.register_for_execution(name="get_complete_schema")(get_complete_schema)

        # Register the preferred data dictionary tools ONLY if they are available
        if settings.METADATA_AVAILABLE:
            logger.info("METADATA_AVAILABLE is True. Registering data dictionary tools.")
            schema_analyst.register_for_llm(name="get_data_dictionary_tables", description="Get descriptions of all available data tables.")(get_data_dictionary_tables)
            user_proxy.register_for_execution(name="get_data_dictionary_tables")(get_data_dictionary_tables)
            
            column_selector.register_for_llm(name="get_data_dictionary_columns", description="Get detailed column descriptions for a list of tables.")(get_data_dictionary_columns)
            user_proxy.register_for_execution(name="get_data_dictionary_columns")(get_data_dictionary_columns)
        else:
            logger.info("METADATA_AVAILABLE is False. Skipping registration of data dictionary tools.")

        # The validator needs to execute SQL, which is always available
        sql_validator.register_for_llm(name="query_database", description="Execute a SQL query and get results.")(query_database)
        user_proxy.register_for_execution(name="query_database")(query_database)

        return {
            "SchemaAnalyst": schema_analyst,
            "ColumnSelector": column_selector,
            "SQLGenerator": sql_generator,
            "SQLValidator": sql_validator,
            "FinalSelector": final_selector,
            "UserProxy": user_proxy, # The proxy is needed to trigger tool execution
        }
