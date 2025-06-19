import logging
import uuid
import datetime
from fastapi import FastAPI, Request, Response 
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
import os
import sys 
import io  

from config.settings import get_settings
from api.routes.agent_sequential import router as agent_router 
from tools.db import init_db_pool, close_db_pool   

settings = get_settings()
LOG_DIR_FOR_REQUESTS = getattr(settings, "REQUEST_LOGS_DIR", "full_request_logs") 
if not os.path.exists(LOG_DIR_FOR_REQUESTS):
    os.makedirs(LOG_DIR_FOR_REQUESTS, exist_ok=True)

LOG_FORMAT_STRING = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    format=LOG_FORMAT_STRING,
)
app_main_logger = logging.getLogger(__name__)
app_main_logger.info(f"Logging full request output to directory: {LOG_DIR_FOR_REQUESTS}")


class CaptureOutputToFile:
    def __init__(self, filepath, also_to_console=True):
        self.filepath = filepath
        self.also_to_console = also_to_console
        self.original_stdout = sys.stdout
        self.original_stderr = sys.stderr
        self.file_handle = None

    def __enter__(self):
        self.file_handle = open(self.filepath, 'a', encoding='utf-8')
        
        class Tee(io.TextIOBase):
            def __init__(self, stream1, stream2):
                self.stream1 = stream1
                self.stream2 = stream2
            def write(self, s):
                written_count = 0
                try:
                    if self.stream1 and not self.stream1.closed:
                        self.stream1.write(s)
                        written_count = len(s)
                except Exception: pass 
                try:
                    if self.stream2 and not self.stream2.closed:
                        self.stream2.write(s)
                        if written_count == 0: written_count = len(s) 
                except Exception: pass
                return written_count
            def flush(self):
                try:
                    if self.stream1 and not self.stream1.closed: self.stream1.flush()
                except Exception: pass
                try:
                    if self.stream2 and not self.stream2.closed: self.stream2.flush()
                except Exception: pass
            def isatty(self): 
                if self.stream2 and hasattr(self.stream2, 'isatty'): return self.stream2.isatty()
                return False

        if self.also_to_console:
            sys.stdout = Tee(self.file_handle, self.original_stdout)
            sys.stderr = Tee(self.file_handle, self.original_stderr)
        else:
            sys.stdout = self.file_handle
            sys.stderr = self.file_handle
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout = self.original_stdout
        sys.stderr = self.original_stderr
        if self.file_handle:
            self.file_handle.close()

app = FastAPI(
    title="Database Agent API",
    description="API for interacting with database agents to execute SQL queries.",
    version="1.0.0",
)

@app.middleware("http")
async def capture_all_output_to_file_middleware(request: Request, call_next):
    is_target_request = False
    target_path_prefix = str(agent_router.prefix) if agent_router.prefix else ""
    # Ensure the path check is precise for your POST query endpoint
    if request.method == "POST" and request.url.path == f"{target_path_prefix}/query":
        is_target_request = True

    if not is_target_request:
        response = await call_next(request)
        return response

    root_logger_instance = logging.getLogger() 
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    req_id = str(uuid.uuid4())[:8]
    output_filename = os.path.join(LOG_DIR_FOR_REQUESTS, f"req_full_output_{timestamp}_{req_id}.log")

    logging_file_handler = logging.FileHandler(output_filename, mode='a', encoding='utf-8')
    logging_file_handler.setFormatter(logging.Formatter(LOG_FORMAT_STRING)) 
    logging_file_handler.setLevel(root_logger_instance.getEffectiveLevel()) 
    root_logger_instance.addHandler(logging_file_handler)
    
    app_main_logger.info(f"--- CAPTURING ALL OUTPUT for req_id {req_id} to: {output_filename} ---")

    # The also_to_console=True means prints still go to terminal
    with CaptureOutputToFile(filepath=output_filename, also_to_console=True):
        try:
            print(f"--- STDOUT/STDERR REDIRECTION ACTIVE (ReqID: {req_id}, Time: {datetime.datetime.now()}) ---", file=sys.stdout)
            response = await call_next(request) 
            print(f"--- STDOUT/STDERR REDIRECTION ENDED (ReqID: {req_id}, Time: {datetime.datetime.now()}) ---", file=sys.stdout)
            app_main_logger.info(f"--- FINISHED CAPTURING OUTPUT for req_id {req_id}. File: {output_filename} ---")
            return response
        except Exception as e:
            print(f"--- EXCEPTION DURING REDIRECTED BLOCK (ReqID: {req_id}, Time: {datetime.datetime.now()}) ---\n{type(e).__name__}: {e}", file=sys.stderr)
            # The error would have been logged by the root logger to the file if it was a standard log
            # or by the print redirection if it was printed.
            app_main_logger.error(f"--- ERROR DURING REQUEST (req_id {req_id}, output in {output_filename}): {e} ---")
            raise 
        finally:
            root_logger_instance.removeHandler(logging_file_handler)
            logging_file_handler.close()
            app_main_logger.debug(f"--- Logging module file handler for {output_filename} removed. Stdout/stderr restored. ---")

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.include_router(agent_router) 

@app.on_event("startup")
def startup_event():
    app_main_logger.info("Initializing application resources")
    init_db_pool()
    app_main_logger.info("Application startup complete")

@app.on_event("shutdown")
def shutdown_event():
    app_main_logger.info("Application shutting down")
    close_db_pool()
    app_main_logger.info("Resources cleaned up successfully")

@app.get("/health")
def health_check():
    app_main_logger.debug("Health check endpoint called.")
    return {"status": "ok"}

@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    # This log goes to console AND the request-specific file (if active for this request)
    logging.getLogger().error(f"Uncaught exception by generic handler for {request.method} {request.url}: {str(exc)}", exc_info=True)
    return JSONResponse(status_code=500, content={"error": "Internal server error", "detail": str(exc)})

if __name__ == "__main__":
    app_main_logger.info(f"Starting Uvicorn server on {settings.API_HOST}:{settings.API_PORT}")
    print("Im starting the sequential agent API server...")  # This will go to the console and the log file if redirection is active
    uvicorn.run(
        "main_sequential:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=getattr(settings, "RELOAD_APP", False),
        log_config=None,
        log_level=settings.LOG_LEVEL.lower(),
    )
