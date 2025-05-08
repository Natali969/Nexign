from faker import Faker
import random

fake = Faker('ru_RU')
def generate_random_info():
    # генерируем случайные данные для абонента
    new_tariff_id = random.randint(1, 2)

    # случайно выбираем пол абонента
    if gender is None:
        gender = random.choice(['male', 'female'])

    if gender == 'male':
        test_first_name = fake.first_name_male()
        test_last_name = fake.last_name_male()
        test_patronymic = fake.middle_name_male()
    elif gender == 'female':
        test_first_name = fake.first_name_female()
        test_last_name = fake.last_name_female()
        test_patronymic = fake.middle_name_female()
    return new_tariff_id, test_first_name, test_last_name, test_patronymic