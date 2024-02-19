import unittest
from A01793875_A6_2.hotel import Hotel


class TestHotel(unittest.TestCase):
    def setUp(self):
        # Create a sample hotel instance for testing
        self.hotel = Hotel("Test Hotel", "Test Location", 10, 5)

    def test_check_availability(self):
        # Test the check_availability method
        self.assertEqual(self.hotel.check_availability(), 5)

    def test_reserve_room(self):
        # Test the reserve_room method when rooms are available
        self.assertTrue(self.hotel.reserve_room())
        self.assertEqual(self.hotel.rooms_available, 4)

        # Test the reserve_room method when no rooms are available
        self.hotel.rooms_available = 0
        self.assertFalse(self.hotel.reserve_room())
        self.assertEqual(self.hotel.rooms_available, 0)

    def test_update_info(self):
        # Test the update_info method
        self.hotel.update_info("New Test Hotel", "New Test Location", 20)
        self.assertEqual(self.hotel.name, "New Test Hotel")
        self.assertEqual(self.hotel.location, "New Test Location")
        self.assertEqual(self.hotel.capacity, 20)
        self.assertEqual(self.hotel.rooms_available, 20)


    def test_to_dict(self):
        # Test the to_dict method
        expected_dict = {
            'name': 'Test Hotel',
            'location': 'Test Location',
            'capacity': 10,
            'rooms_available': 5
        }
        self.assertEqual(self.hotel.to_dict(), expected_dict)





if __name__ == '__main__':
    unittest.main()
