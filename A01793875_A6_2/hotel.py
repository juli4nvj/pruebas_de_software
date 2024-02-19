"""
Clase Hotel.py
"""


class Hotel:
    """Clase Hotel"""
    def __init__(self, name, location, capacity, rooms_available):
        """Initialize a Hotel instance with the provided attributes."""
        self.name = name
        self.location = location
        self.capacity = capacity
        self.rooms_available = int(rooms_available)

    def check_availability(self):
        """Check the availability of rooms."""
        return self.rooms_available

    def reserve_room(self):
        """Reserve a room if available."""
        value = False
        if self.rooms_available > 0:
            self.rooms_available -= 1
            value = True
        return value

    def cancel_reservation(self):
        """Cancel a reservation and free up a room."""
        value = False
        if self.rooms_available < self.capacity:
            self.rooms_available += 1
            value = True
        return value

    def update_info(self, name, location, capacity):
        """Update the hotel's information."""
        self.name = name
        self.location = location
        self.capacity = capacity
        self.rooms_available = capacity

    def to_dict(self):
        """Convert the hotel object to a dictionary."""
        return {
            'name': self.name,
            'location': self.location,
            'capacity': self.capacity,
            'rooms_available': self.rooms_available
        }
