# pylint: disable=wrong-import-order
# pylint: disable=too-many-branches
"""
Clase Reservation_System.py
"""

import json
import sys
from hotel import Hotel
from reservation import Reservation
from customers import Customer
import os


class Manager:
    """Clase reservationSystem"""

    def __init__(self):
        """
        Initialize Manager with empty lists for hotels,
        reservations, and customers.
        """
        self.hotels = []
        self.reservations = []
        self.customers = []

    def load_data(self):
        """Load data from JSON files if available."""
        try:
            with open('data/hotels.json', 'r', encoding='utf-8') as f:
                self.hotels = [Hotel(**hotel_data) for hotel_data in
                               json.load(f)]
        except FileNotFoundError:
            pass

        try:
            with open('data/reservations.json', 'r', encoding='utf-8') as f:
                self.reservations = [Reservation(**reservation_data) for
                                     reservation_data in
                                     json.load(f)]
        except FileNotFoundError:
            pass

        try:
            with open('data/customers.json', 'r', encoding='utf-8') as f:
                self.customers = [Customer(**customer_data) for customer_data
                                  in json.load(f)]
        except FileNotFoundError:
            pass

    def save_data(self):
        """Save data to JSON files."""
        with open('data/hotels.json', 'w', encoding='utf-8') as f:
            json.dump([hotel.to_dict() for hotel in self.hotels], f, indent=4)

        with open('data/reservations.json', 'w', encoding='utf-8') as f:
            json.dump(
                [reservation.to_dict() for reservation in self.reservations],
                f, indent=4)

        with open('data/customers.json', 'w', encoding='utf-8') as f:
            json.dump([customer.to_dict() for customer in self.customers], f,
                      indent=4)

    def create_hotel(self, name, location, capacity):
        """Create a new hotel."""
        hotel = Hotel(name, location, capacity, capacity)
        self.hotels.append(hotel)
        self.save_data()

    def delete_hotel(self, name):
        """Delete a hotel by name."""
        for hotel in self.hotels:
            if hotel.name == name:
                self.hotels.remove(hotel)
                self.save_data()
                return True
        return False

    def display_hotels(self):
        """Display information about all hotels."""
        for hotel in self.hotels:
            print(
                f"Hotel Name: {hotel.name}, Location: {hotel.location}, "
                f"Capacity: {hotel.capacity}, "
                f"Available rooms: {hotel.rooms_available}")

    def modify_hotel_info(self, name, new_name, new_location, new_capacity):
        """Update hotel information."""
        for hotel in self.hotels:
            if hotel.name == name:
                hotel.update_info(new_name, new_location, new_capacity)
                self.save_data()
                return True
        return False

    def reserve_room(self, customer_name, hotel_name, num_bedrooms):
        """Reserve a room in a hotel for a customer."""
        value = False
        for hotel in self.hotels:
            if hotel.name == hotel_name:
                if hotel.reserve_room():
                    reservation = Reservation(customer_name, hotel_name,
                                              num_bedrooms)
                    self.reservations.append(reservation)
                    self.save_data()
                    value = True
                return value
        return False

    def cancel_reservation(self, customer_name, hotel_name):
        """Cancel a reservation made by a customer in a hotel."""
        for reservation in self.reservations:
            if (reservation.customer_name == customer_name
                    and reservation.hotel_name == hotel_name):
                self.reservations.remove(reservation)
                for hotel in self.hotels:
                    if hotel.name == hotel_name:
                        hotel.cancel_reservation()
                        self.save_data()
                        return True
        return False

    def display_reservation(self):
        """Display information about all reservations."""
        for reservation in self.reservations:
            print(
                f"Name: {reservation.customer_name}, "
                f"Nombre Hotel: {reservation.hotel_name}, "
                f"Rooms: {reservation.num_bedrooms}")

    def create_customer(self, name, email, phone):
        """Create a new customer."""
        customer = Customer(name, email, phone)
        self.customers.append(customer)
        self.save_data()

    def delete_customer(self, name):
        """Delete a customer by name."""
        for customer in self.customers:
            if customer.name == name:
                self.customers.remove(customer)
                self.save_data()
                return True
        return False

    def display_customers(self):
        """Display information about all customers."""
        for customer in self.customers:
            print(
                f"Name: {customer.name}, Email: {customer.email}, "
                f"Phone: {customer.phone}")

    def modify_customer_info(self, name, new_name, new_email, new_phone):
        """Modify customer information."""
        for customer in self.customers:
            if customer.name == name:
                customer.update_info(new_name, new_email, new_phone)
                self.save_data()
                return True
        return False


def main():
    """Main function to run the hotel management system."""
    manager = Manager()
    manager.load_data()

    while True:
        print("\nMenu:")
        print("1. Hotels")
        print("2. Customers")
        print("3. Reservations")
        print("4. Exit")

        choice = input("Enter your choice: ")

        if choice == "1":
            clear_screen()
            hotel_menu(manager)
        elif choice == "2":
            clear_screen()
            customer_menu(manager)
        elif choice == "3":
            clear_screen()
            reservation_menu(manager)
        elif choice == "4":
            clear_screen()
            os.system('exit()')
            sys.exit()
        else:
            print("Invalid choice. Please choose again.")


def options_hotel_menu(principal, option):
    """Options for the hotel menu"""
    if option == "1":
        name = input("\nEnter hotel name: ")
        location = input("Enter location: ")
        capacity = int(input("Enter bedrooms: "))
        principal.create_hotel(name, location, capacity)
    elif option == "2":
        name = input("\nEnter hotel name to delete: ")
        if principal.delete_hotel(name):
            print("Hotel deleted successfully.")
        else:
            print("Hotel not found.")
    elif option == "3":
        principal.display_hotels()
    elif option == "4":
        name = input("\nEnter hotel name to modify: ")
        new_name = input("Enter new name (press Enter to skip): ")
        new_location = input("Enter new location (press Enter to skip): ")
        new_capacity = input("Enter new bedrooms (press Enter to skip): ")
        if principal.modify_hotel_info(name, new_name, new_location,
                                       new_capacity):
            print("Hotel information modified successfully.")
        else:
            print("Hotel not found.")
    elif option == "5":
        customer_name = input("\nEnter customer name: ")
        hotel_name = input("Enter hotel name: ")
        if principal.reserve_room(customer_name, hotel_name):
            print("Room reserved successfully.")
        else:
            print(
                "Room reservation failed. Either hotel not found or "
                "no rooms available.")
    elif option == "6":
        customer_name = input("\nEnter customer name: ")
        hotel_name = input("Enter hotel name: ")
        if principal.cancel_reservation(customer_name, hotel_name):
            print("Reservation cancelled successfully.")
        else:
            print("Reservation cancellation failed.")
    elif option == "7":
        clear_screen()
        main()
    else:
        print("Invalid option.")


def options_customer_menu(principal, option):
    """Options for the customer menu."""
    if option == "1":
        name = input("\nEnter customer name: ")
        email = input("Enter email: ")
        phone = input("Enter phone: ")
        principal.create_customer(name, email, phone)
    elif option == "2":
        name = input("\nEnter customer name to delete: ")
        if principal.delete_customer(name):
            print("Customer deleted successfully.")
        else:
            print("Customer not found.")
    elif option == "3":
        principal.display_customers()
    elif option == "4":
        name = input("\nEnter customer name to modify: ")
        new_name = input("Enter new name (press Enter to skip): ")
        new_email = input("Enter new email (press Enter to skip): ")
        new_phone = input("Enter new phone (press Enter to skip): ")
        if principal.modify_customer_info(name, new_name, new_email,
                                          new_phone):
            print("Customer information modified successfully.")
        else:
            print("Customer not found.")
    elif option == "5":
        clear_screen()
        main()
    else:
        print("Invalid option.")


def options_reservation_menu(principal, option):
    """Options for the reservation menu."""
    if option == "1":
        customer_name = input("\nEnter customer name: ")
        hotel_name = input("Enter hotel name: ")
        num_bedrooms = input("Enter number bedrooms: ")
        if principal.reserve_room(customer_name, hotel_name, num_bedrooms):
            print("Reservation created successfully.")
        else:
            print(
                "Reservation creation failed. Either hotel not found or "
                "no rooms available.")
    elif option == "2":
        customer_name = input("\nEnter customer name: ")
        hotel_name = input("Enter hotel name: ")
        if principal.cancel_reservation(customer_name, hotel_name):
            print("Reservation cancelled successfully.")
        else:
            print("Reservation cancellation failed.")
    elif option == "3":
        principal.display_reservation()
    elif option == "4":
        clear_screen()
        main()
    else:
        print("Invalid option.")


def menu_hotel():
    """Display the hotel menu."""
    print("\nHotel Menu:")
    print("1. Create Hotel")
    print("2. Delete Hotel")
    print("3. Display Hotel information")
    print("4. Modify Hotel Information")
    print("5. Reserve a Room")
    print("6. Cancel a Reservation")
    print("7. Back to Main Menu")


def menu_customer():
    """Display the customer menu."""
    print("\nCustomer Menu:")
    print("1. Create Customer")
    print("2. Delete Customer")
    print("3. Display Customer Information")
    print("4. Modify Customer Information")
    print("5. Back to Main Menu")


def menu_reservation():
    """Display the reservation menu."""
    print("\nReservation Menu:")
    print("1. Create a Reservation")
    print("2. Cancel a Reservation")
    print("3. Display Reservation")
    print("4. Back to Main Menu")


def hotel_menu(principal):
    """Display and handle options for the hotel menu."""
    menu_hotel()
    while True:
        option = input("\nSelect option: ")
        clear_screen()
        menu_hotel()
        options_hotel_menu(principal, option)


def customer_menu(principal):
    """Display and handle options for the customer menu."""
    menu_customer()
    while True:
        option = input("\nSelect option: ")
        clear_screen()
        menu_customer()
        options_customer_menu(principal, option)


def reservation_menu(principal):
    """Display and handle options for the reservation menu."""
    menu_reservation()
    while True:
        option = input("\nSelect option: ")
        clear_screen()
        menu_reservation()
        options_reservation_menu(principal, option)


def clear_screen():
    """Clear the screen."""
    os.system('cls')


if __name__ == "__main__":
    main()
