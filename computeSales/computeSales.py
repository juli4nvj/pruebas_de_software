import json
import sys
import time


def read_file(filename):
    try:
        with open(filename, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        return None
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in '{filename}'.")
        return None


def f_calculate_cost(catalogue, sales):
    total = 0
    for sale in sales:
        product = sale.get("Product")
        quantity = sale.get("Quantity")
        for sales in catalogue:
            title = sales.get("title")
            if product == title:
                price = sales.get("price")
                total += price * quantity
        else:
            print(f"The product {product} not found.")

    return total


def main():
    if len(sys.argv) < 2:
        print("Parameters missing")
        sys.exit(1)

    start_time = time.time()
    total_list = []

    total_sale = 0
    count_file = 0

    for i in range(1, len(sys.argv), 2):
        catalogue_file = sys.argv[i]
        sales_file = sys.argv[i + 1]

        catalogue = read_file(catalogue_file)
        sales = read_file(sales_file)

        if catalogue is None or sales is None:
            sys.exit(1)

        count_file += 1
        total_sale = f_calculate_cost(catalogue, sales)
        result = f'TC{count_file} {total_sale:.2f}'
        total_list.append(result)

    end_time = time.time()
    elapsed_time = end_time - start_time
    result = "    TOTAL\n" + "\n".join(total_list) + "\n"
    print(result)
    print(f"Elapsed time: {elapsed_time:.2f} seconds")

    with open("SalesResults.txt", 'w') as results_file:
        results_file.write(result)


if __name__ == "__main__":
    main()
