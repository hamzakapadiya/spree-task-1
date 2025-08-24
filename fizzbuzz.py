#!/usr/bin/env python3

def fizzBuzz(n):
    """
    Prints FizzBuzz sequence from 1 to n inclusive.
    
    Args:
        n: upper limit of values to test (inclusive)
    
    Returns:
        None (prints to stdout)
    """
    for i in range(1, n + 1):
        if i % 3 == 0 and i % 5 == 0:
            print("FizzBuzz")
        elif i % 3 == 0:
            print("Fizz")
        elif i % 5 == 0:
            print("Buzz")
        else:
            print(i)


# Example usage and testing
if __name__ == "__main__":
    # Test with n = 15
    print("Testing fizzBuzz(15):")
    fizzBuzz(15)
    
    print("\nTesting fizzBuzz(20):")
    fizzBuzz(20)