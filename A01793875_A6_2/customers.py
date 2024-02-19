"""
Clase Customers.py
"""


class Customer:
    """Clase Customer"""
    def __init__(self, name, email, phone):
        """Initialize a Customer instance with the provided attributes."""
        self.name = name
        self.email = email
        self.phone = phone

    def update_info(self, name, email, phone):
        """Update the customer's information."""
        self.name = name
        self.email = email
        self.phone = phone

    def to_dict(self):
        """Convert the customer object to a dictionary."""
        return {
            'name': self.name,
            'email': self.email,
            'phone': self.phone
        }
