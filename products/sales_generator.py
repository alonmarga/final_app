from random import choice, randint
from uuid import uuid4
from datetime import datetime, timedelta
import logging


def generate_mock_purchase_data(personal_data, max_num_purchases):
    logger = logging.getLogger("main_logger.generate_func")
    logger.info("generate_mock_purchase_data function called.")
    logger.info(f"Max number of sales: {max_num_purchases}")
    departments = {
        'Electronics': {
            'id': 'DEP001',
            'sub_departments': [
                {
                    'id': 'SUBDEP001',
                    'name': 'TVs',
                    'products': [
                        {'id': 'PRO001', 'name': 'TV', 'price': 500}
                    ]
                },
                {
                    'id': 'SUBDEP002',
                    'name': 'Laptops',
                    'products': [
                        {'id': 'PRO002', 'name': 'Laptop', 'price': 1000}
                    ]
                },
                {
                    'id': 'SUBDEP003',
                    'name': 'Smartphones',
                    'products': [
                        {'id': 'PRO003', 'name': 'Smartphone', 'price': 800}
                    ]
                }
            ]
        },
        'Clothing': {
            'id': 'DEP002',
            'sub_departments': [
                {
                    'id': 'SUBDEP004',
                    'name': 'Shirts',
                    'products': [
                        {'id': 'PRO004', 'name': 'Shirt', 'price': 20}
                    ]
                },
                {
                    'id': 'SUBDEP005',
                    'name': 'Dresses',
                    'products': [
                        {'id': 'PRO005', 'name': 'Dress', 'price': 50}
                    ]
                },
                {
                    'id': 'SUBDEP006',
                    'name': 'Jeans',
                    'products': [
                        {'id': 'PRO006', 'name': 'Jeans', 'price': 40}
                    ]
                }
            ]
        },
        'Home & Kitchen': {
            'id': 'DEP003',
            'sub_departments': [
                {
                    'id': 'SUBDEP007',
                    'name': 'Cookware',
                    'products': [
                        {'id': 'PRO007', 'name': 'Cookware Set', 'price': 200}
                    ]
                },
                {
                    'id': 'SUBDEP008',
                    'name': 'Appliances',
                    'products': [
                        {'id': 'PRO008', 'name': 'Blender', 'price': 80}
                    ]
                },
                {
                    'id': 'SUBDEP009',
                    'name': 'Coffee Makers',
                    'products': [
                        {'id': 'PRO009', 'name': 'Coffee Maker', 'price': 150}
                    ]
                }
            ]
        },
        'Books': {
            'id': 'DEP004',
            'sub_departments': [
                {
                    'id': 'SUBDEP010',
                    'name': 'Fiction',
                    'products': [
                        {'id': 'PRO010', 'name': 'Novel', 'price': 15}
                    ]
                },
                {
                    'id': 'SUBDEP011',
                    'name': 'Non-Fiction',
                    'products': [
                        {'id': 'PRO011', 'name': 'Non-Fiction', 'price': 25}
                    ]
                },
                {
                    'id': 'SUBDEP012',
                    'name': 'Children',
                    'products': [
                        {'id': 'PRO012', 'name': "Children's Book", 'price': 10}
                    ]
                }
            ]
        },
        'Beauty': {
            'id': 'DEP005',
            'sub_departments': [
                {
                    'id': 'SUBDEP013',
                    'name': 'Perfumes',
                    'products': [
                        {'id': 'PRO013', 'name': 'Perfume', 'price': 50}
                    ]
                },
                {
                    'id': 'SUBDEP014',
                    'name': 'Makeup',
                    'products': [
                        {'id': 'PRO014', 'name': 'Makeup', 'price': 30}
                    ]
                },
                {
                    'id': 'SUBDEP015',
                    'name': 'Skincare',
                    'products': [
                        {'id': 'PRO015', 'name': 'Skincare', 'price': 40}
                    ]
                }
            ]
        }
    }

    mock_purchase_data = []

    for data in personal_data:
        customer_id = data['id']
        first_name = data['first_name']
        last_name = data['last_name']
        email = data['email']
        country = data['country']
        card_type = data['card_type']
        gender = data['gender']
        currency = data['currency']
        club = data['club']
        online = data['online']
        country_code = data['country_code']

        num_purchases = randint(1, max_num_purchases)

        for _ in range(num_purchases):
            purchase_id = str(uuid4())
            purchase_time = generate_random_purchase_time()

            department_count = randint(1, len(departments))
            selected_departments = set()

            while len(selected_departments) < department_count:
                department = choice(list(departments.keys()))
                selected_departments.add(department)

            for department in selected_departments:
                department_id = departments[department]['id']
                sub_departments_in_department = departments[department]['sub_departments']
                selected_sub_departments = set()

                while len(selected_sub_departments) < len(sub_departments_in_department):
                    sub_department = choice(sub_departments_in_department)
                    selected_sub_departments.add(sub_department['id'])

                for sub_department_id in selected_sub_departments:
                    sub_department = next((sd for sd in sub_departments_in_department if sd['id'] == sub_department_id),
                                          None)

                    if sub_department:
                        products_in_sub_department = sub_department['products']
                        product = choice(products_in_sub_department)
                        product_id = product['id']
                        product_name = product['name']
                        product_price = product['price']
                        quantity = randint(1, 5)

                        purchase_data = {
                            'purchase_id': purchase_id,
                            'customer_id': customer_id,
                            'first_name': first_name,
                            'last_name': last_name,
                            'country': country,
                            'club': club,
                            'card_type': card_type,
                            'gender': gender,
                            'currency': currency,
                            'email': email,
                            'product_id': product_id,
                            'product_name': product_name,
                            'product_price': product_price,
                            'quantity': quantity,
                            'department_id': department_id,
                            'department_name': department,
                            'sub_department_id': sub_department_id,
                            'sub_department_name': sub_department['name'],
                            'purchase_time': purchase_time,
                            'online': online,
                            'country_code': country_code,
                        }

                        mock_purchase_data.append(purchase_data)
    logger.info("Finished creating sales")
    return mock_purchase_data


def generate_random_purchase_time():
    start_date = datetime.now() - timedelta(days=365)
    end_date = datetime.now()
    random_seconds = randint(0, int((end_date - start_date).total_seconds()))
    purchase_time = start_date + timedelta(seconds=random_seconds)
    return purchase_time.strftime('%Y-%m-%d %H:%M:%S')
