# pylint: disable=invalid-name
"""
Ejercicio 2
Code to convert numbers to bin and hex
"""

import sys
import time

def convert_number(number):
    """Convert numbers to bin and hex"""
    try:
        number = int(number)
        binary = bin(number & 0xFFFFFFFFFFFFFFFF)[2:]
        hexa = hex(number & 0xFFFFFFFFFFFFFFFF)[2:].upper()
        binary = binary[-10:].lstrip('0') or '0'
        hexa = hexa[-10:].lstrip('0') or '0'
        return binary, hexa
    except ValueError:
        return "#VALUE!", "#VALUE!"


def main():
    """main general"""
    start_time = time.time()

    if len(sys.argv) < 2:
        print("Error: faltan parametros.")
        sys.exit(1)

    file_paths = sys.argv[1:]
    result_path = "ConvertionResults.txt"

    with open(result_path, 'w', encoding='utf-8') as result_file:
        result_file.write("NUMBER\tTC\tBIN\tHEX\n")

        for file_path in file_paths:
            path_file = file_path.split('.')[0]
            result_file.write("\n")
            result_file.write(f"NUMBER\t{path_file}\tBIN\tHEX\n")
            print(f"\nNUMBER\t{path_file}\tBIN\tHEX")

            with open(file_path, 'r', encoding='utf-8') as file:
                numbers = []
                for line in file:
                    numbers.append(line.strip())
            if numbers is not None:
                for i, number in enumerate(numbers, start=1):
                    binary, hexa = convert_number(number)
                    result_file.write(f"{i}\t{number}\t{binary}\t{hexa}\n")
                    print(f"{i}\t{number}\t{binary}\t{hexa}")

    elapsed_time = time.time() - start_time
    print(f"\nElapsed Time: {elapsed_time} seconds")


if __name__ == "__main__":
    main()
