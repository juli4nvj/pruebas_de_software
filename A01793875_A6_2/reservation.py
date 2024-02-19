"""
Clase reservation.py
"""


class Reservation:
    """Class to represent a reservation."""

    def __init__(self, customer_name, hotel_name, num_bedrooms):
        """Initialize a Reservation instance with the provided attributes."""
        self.customer_name = customer_name
        self.hotel_name = hotel_name
        self.num_bedrooms = num_bedrooms

    def to_dict(self):
        """Convert the reservation object to a dictionary."""
        return {
            'customer_name': self.customer_name,
            'hotel_name': self.hotel_name,
            'num_bedrooms': self.num_bedrooms
        }

    def update_details(self, new_customer_name, new_num_bedrooms):
        """Update the details of the reservation."""
        self.customer_name = new_customer_name
        self.num_bedrooms = new_num_bedrooms
