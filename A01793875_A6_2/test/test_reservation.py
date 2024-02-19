import unittest
from A01793875_A6_2.reservation import Reservation


class TestReservation(unittest.TestCase):
    def test_init(self):
        reservation = Reservation("John Doe", "Hotel ABC", 2)
        self.assertEqual(reservation.customer_name, "John Doe")
        self.assertEqual(reservation.hotel_name, "Hotel ABC")
        self.assertEqual(reservation.num_bedrooms, 2)

    def test_to_dict(self):
        reservation = Reservation("Jane Doe", "Hotel XYZ", 3)
        expected_dict = {
            'customer_name': "Jane Doe",
            'hotel_name': "Hotel XYZ",
            'num_bedrooms': 3
        }
        self.assertEqual(reservation.to_dict(), expected_dict)


if __name__ == '__main__':
    unittest.main()
