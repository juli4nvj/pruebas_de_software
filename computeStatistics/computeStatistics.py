# pylint: disable=invalid-name
"""
Ejercicio 1
Code to calculate multimple statistics
"""
import sys
import time


def f_read_file(path):
    """
    Function to read file
    """
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = [line.strip() for line in f]
        return data
    except FileNotFoundError:
        print(f"Error: File not found - {path}")
        sys.exit(1)
    except UnicodeDecodeError:
        print(f"Error: Unable to decode file content - {path}")
        sys.exit(1)


def f_mode(data):
    """
    Function to calculate mode
    """
    try:
        frequency_dict = {}
        for value in data:
            frequency_dict[value] = frequency_dict.get(value, 0) + 1
        max_frequency = max(frequency_dict.values())
        mode_values = [key for key, value in frequency_dict.items() if value == max_frequency]
        return int(mode_values[0] if len(mode_values) > 1 else mode_values[0])
    except (ValueError, IndexError) as e:
        print(f"Error in calculating mode: {e}")
        return None


def f_numeric(value):
    """
    Function to convert to numeric
    """
    try:
        return float(value)
    except ValueError:
        return value


def f_statistics(file, data):
    """
    Function to calculate statistics
    """
    try:
        data = [f_numeric(value) for value in data]
        count = len(data)
        data1 = [float(value) for value in data if isinstance(value, (int, float))]
        mean = round(sum(data1) / len(data1), 7)
        mid = len(data1) // 2
        sort_data = sorted(data1)
        median = (sort_data[mid] + sort_data[-mid - 1]) / 2 if len(data1) % 2 == 0 else sort_data[
            mid]
        mode = f_mode(data1)
        dif_squad = [(x - mean) ** 2 for x in data1]
        variance = round(sum(dif_squad) / (len(data1) - 1), 4)
        sd = round((sum(dif_squad) / len(data1)) ** 0.5, 7)
        return {"file": file, "COUNT": count, "MEAN": mean, "MEDIAN": median, "MODE": mode,
                "SD": sd,
                "VARIANCE": variance}
    except (ValueError, ZeroDivisionError, IndexError) as e:
        print(f"Error in calculating statistics: {e}")
        return {}


def f_print_results(results, columns, stats):
    """
    Function to print result
    """
    try:
        print("\t".join(columns))
        for stat in stats:
            print(stat, end="\t")
            for result in results:
                try:
                    print(result[stat], end="\t")
                except KeyError as key_error:
                    print(f"Error: Missing key '{stat}' in result dictionary - {key_error}")
            print()
    except TypeError as type_error:
        print(f"Error: {type_error}")


def f_save_file(all_results, columns, stats, path, end):
    """
    Function to save file
    """
    try:
        with open(path, 'w', encoding='utf-8') as file:
            file.write("\t".join(columns) + "\n")
            for stat in stats:
                file.write(stat)
                for result in all_results:
                    try:
                        file.write("\t" + str(result[stat]))
                    except KeyError as key_error:
                        print(f"Error: Missing key '{stat}' in result dictionary - {key_error}")
                file.write("\n")
            file.write("\nElapsed Time:\t" + str(end))
    except FileNotFoundError as file_not_found_error:
        print(f"Error: File not found - {file_not_found_error}")
    except PermissionError as permission_error:
        print(f"Error: Permission error - {permission_error}")


def main():
    """Main principal"""
    try:
        if len(sys.argv) < 2:
            print("Error: faltan parametros")
            sys.exit(1)

        start = time.time()
        final_data = []

        for i in sys.argv[1:]:
            path_file = i.split('.')[0]
            process_data = f_read_file(i)
            data_final = f_statistics(path_file, process_data)
            final_data.append(data_final)

        print_columns = ["TC", "TC1", "TC2", "TC3", "TC4", "TC5", "TC6", "TC7"]
        print_stats = ["COUNT", "MEAN", "MEDIAN", "MODE", "SD", "VARIANCE"]
        NAME_FILE = "StatisticsResults.txt"
        f_print_results(final_data, print_columns, print_stats)
        end = time.time() - start
        f_save_file(final_data, print_columns, print_stats, NAME_FILE, end)
        print("Elapsed Time: ", end)
    except (FileNotFoundError, IOError, ValueError, TypeError) as error:
        print(f"Error: {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()
