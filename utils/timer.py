import time

def timer(enabled=True):
    def decorator(func):
        def wrapper(*args, **kwargs):
            if enabled:
                start_time = time.time()
                result = func(*args, **kwargs)
                end_time = time.time()
                print(f"Function '{func.__name__}' took {end_time - start_time:.6f} seconds to run.")
            else:
                result = func(*args, **kwargs)
            return result
        return wrapper
    return decorator