# Decorator to swap values
def swap_decorator(func):
    def wrapper():
        # Get the values from the generator
        x, y = func()
        print(f"Before Swapping: x = {x}, y = {y}")
        # Swap the values
        x, y = y, x
        print(f"After Swapping: x = {x}, y = {y}")
        return x, y
    return wrapper

# Generator that yields two values
@swap_decorator
def value_generator():
    # Values to be swapped
    x = 10
    y = 20
    yield x
    yield y

# Execute the generator and swap values
value_generator()
