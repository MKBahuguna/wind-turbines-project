import signal

log_manager = LogManager()

# Set up the signal handler
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

log_manager.start_capture()

try:
    # Your main job logic here
    pass  # Replace with actual logic
except Exception as e:
    log_manager.get_logger("YourLoggerName").error(f"An error occurred: {e}")
finally:
    log_manager.write_audit_logs_to_delta(delta_table_path, args, layer)
    log_manager.stop_capture()  # Stop capturing logs