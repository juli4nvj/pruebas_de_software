# pylint: disable=invalid-name
"""
Ejercicio 3
Code to Count Words
"""
import sys
import time

def process_file(file_path, file_index):
    """Process data"""
    words_count = {}
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                words = line.strip().split()
                for word in words:
                    key = f"TC{file_index}"
                    if key not in words_count:
                        words_count[key] = {}
                    if word in words_count[key]:
                        words_count[key][word] += 1
                    else:
                        words_count[key][word] = 1
        return words_count
    except IOError as e:
        print(f"Error reading file {file_path}: {e}")
        return None

def process_files(file_paths):
    """Process files"""
    all_words_count = {}

    for i, file_path in enumerate(file_paths, start=1):
        words_count = process_file(file_path, i)
        if words_count is not None:
            for key, counts in words_count.items():
                if key in all_words_count:
                    f_update_count(all_words_count, key, counts)
                else:
                    all_words_count[key] = counts
    return all_words_count


def f_update_count(all_words_count, key, counts):
    """Update the all_words_count dictionary"""
    for word, count in counts.items():
        if word in all_words_count[key]:
            all_words_count[key][word] += count
        else:
            all_words_count[key][word] = count



def main():
    """Process main"""
    start_time = time.time()

    if len(sys.argv) < 2:
        print("Error: Missing file parameters.")
        sys.exit(1)

    file_paths = sys.argv[1:]
    result_path = "WordCountResults.txt"

    words_count = process_files(file_paths)

    if words_count:
        with open(result_path, 'w', encoding='utf-8') as result_file:
            for key, counts in words_count.items():
                result_file.write(f"Row Labels\tCount of {key}\n")
                print(f"Row Labels\tCount of {key}")
                local_total = 0
                for word, count in counts.items():
                    result_file.write(f"{word}\t{count}\n")
                    print(f"{word}\t{count}")
                    local_total += count
                result_file.write(f"(blank)\t\n" f"Grand Total {key}\t{local_total}\n\n")
                print(f"(blank)\n" f"Grand Total {key}\t{local_total}\n")
        elapsed_time = time.time() - start_time
        with open(result_path, 'a', encoding='utf-8') as result_file:
            result_file.write(f"Elapsed Time\t{elapsed_time} seconds\n")
        print(f"Elapsed Time: {elapsed_time} seconds")


if __name__ == "__main__":
    main()
