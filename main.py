import psycopg2


def create_tables():
    with psycopg2.connect(database="study_db", user="postgres", password="qwerty") as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE person_name(
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(40) NOT NULL,
            last_name VARCHAR(40) NOT NULL
            );
            """)

            cur.execute("""
            CREATE TABLE phone_number(
            id SERIAL PRIMARY KEY,
            person_name_id INTEGER NOT NULL REFERENCES person_name(id),
            number DECIMAL(11,0) UNIQUE
            );
            """)

            cur.execute("""
            CREATE TABLE email(
            id SERIAL PRIMARY KEY,
            person_name_id INTEGER NOT NULL REFERENCES person_name(id),
            email VARCHAR(40) NOT NULL UNIQUE
            );
            """)
            print('Таблицы созданы')

def add_customer(first_name, last_name, email, phone=None):
    with psycopg2.connect(database="study_db", user="postgres", password="qwerty") as conn:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO person_name(first_name, last_name) VALUES(%s, %s) RETURNING id;
            """, (first_name, last_name))
            person_name_id = cur.fetchone()[0]

            cur.execute("""
            INSERT INTO phone_number(person_name_id, number) VALUES(%s, %s);
            """, (person_name_id, phone))

            cur.execute("""
            INSERT INTO email(person_name_id, email) VALUES(%s, %s);
            """, (person_name_id, email))
            print('Клиент занесён в базу')

def add_new_phone(person_name_id, phone_number):
    with psycopg2.connect(database="study_db", user="postgres", password="qwerty") as conn:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO phone_number(person_name_id, number)
            VALUES(%s, %s);
            """, (person_name_id, phone_number))
            print('Номер телефона добавлен')

def change_data(person_name_id=None, first_name=None, last_name=None,
                email_id=None, email=None, phone_id=None, phone=None):
    with psycopg2.connect(database="study_db", user="postgres", password="qwerty") as conn:
        with conn.cursor() as cur:
            if first_name:
                cur.execute("""
                UPDATE person_name SET first_name=%s WHERE id=%s;
                """, (first_name, person_name_id))
            if last_name:
                cur.execute("""
                UPDATE person_name SET last_name=%s WHERE id=%s;
                """, (last_name, person_name_id))
            if phone:
                cur.execute("""
                UPDATE phone_number SET number=%s WHERE id=%s;
                """, (phone, phone_id))
            if email:
                cur.execute("""
                UPDATE email SET email=%s WHERE id=%s;
                """, (email, email_id))
            if cur.rowcount > 0:
                print('Данные о клиенте успешно изменены')
            else:
                print('Ошибка. Данные не заменены. Возможно клиент отсутствует в базе')

def dell_phone(phone_id):
    with psycopg2.connect(database="study_db", user="postgres", password="qwerty") as conn:
        with conn.cursor() as cur:
            cur.execute("""
            DELETE FROM phone_number WHERE id=%s;
            """, (phone_id, ))
            if cur.rowcount > 0:
                print('Номер телефона успешно удалён')
            else:
                print('Ошибка. Данные не изменены. Возможно номер телефона отсутствует в базе')

def delete_customer(person_name_id):
    with psycopg2.connect(database="study_db", user="postgres", password="qwerty") as conn:
        with conn.cursor() as cur:
            cur.execute("""
            DELETE FROM phone_number WHERE person_name_id=%s;
            """, (person_name_id, ))

            cur.execute("""
            DELETE FROM email WHERE person_name_id=%s;
            """, (person_name_id, ))

            cur.execute("""
            DELETE FROM person_name WHERE id=%s;
            """, (person_name_id, ))
            if cur.rowcount > 0:
                print('Данные о клиенте успешно удалены')
            else:
                print('Ошибка. Запись отсутствует в базе')

def print_search_result(result):
    if len(result) == 0:
        print('Клиент не найден')
    else:
        print(f'Name: {result[0][0]}',
              f'Surname: {result[0][1]}', sep='\n')
        for i in range(len(result)):
            print(f'Phone number {i + 1}: {result[i][2]}')
            if result[i][2] == result[i-1][2]:
                break
        for i in range(len(result)):
            print(f'email {i + 1}: {result[i][3]}')
            if result[i][3] == result[i-1][3]:
                break

def search_customer(first_name='%', last_name='%',
                email='%', phone='%'):
    with psycopg2.connect(database="study_db", user="postgres", password="qwerty") as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT pn.first_name, pn.last_name, p.number, e.email
            FROM person_name pn
            JOIN phone_number p
            ON pn.id = p.person_name_id
            JOIN email e
            ON pn.id = e.person_name_id
            AND pn.first_name ILIKE %s
            AND pn.last_name ILIKE %s
            AND e.email ILIKE %s
            AND COALESCE(CAST(p.number AS TEXT), '-') ILIKE %s;
            """, (first_name + '%', last_name + '%',
                email + '%', str(phone) + '%'))
            result = cur.fetchall()
            print_search_result(result)



if __name__ == "__main__":
# CREATE TABLEs
    try:
        create_tables()
    except psycopg2.errors.DuplicateTable:
        conn.rollback()
        print('Ошибка создания таблиц')
    input('Press enter to continue')
# ADD Customers
    try:
        add_customer('Ivan', 'Ivanov',
                 'ivan@pochta.ru', 89083457284)
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        print('Ошибка добавления. Клиент уже существуют')
    input('Press enter to continue')

    try:
        add_customer('Petr', 'Petrov',
                 'petr@mail.ru', 81112223334)
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        print('Ошибка добавления. Клиент уже существуют')
    input('Press enter to continue')

    try:
        add_customer('Sergey', 'Sergeev',
                 'sergey@pochta-mail.ru', 89090001199)
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        print('Ошибка добавления. Клиент уже существуют')
    input('Press enter to continue')

    try:
        add_customer('Olga', 'Buzova',
                 'buzovastar@super.ru')
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        print('Ошибка добавления. Клиент уже существуют')
    input('Press enter to continue')
# ADD Customer's phone
    try:
        add_new_phone(1, 85557771212)
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        print('Ошибка добавления. Номер телефона уже существуют')
    input('Press enter to continue')
# Change any customer's data
    change_data(person_name_id=3, first_name='Mikhail', last_name=None,
            email_id=None, email=None, phone_id=None, phone=None)
    input('Press enter to continue')
# Delete phone
    dell_phone(2)
    input('Press enter to continue')
# Delete customer
    delete_customer(2)
    input('Press enter to continue')
# Search scenarios
    search_customer(first_name='olga')
    input('Press enter to continue')

    search_customer(first_name='mikhail', last_name= 's')
    input('Press enter to continue')

    search_customer(phone='89083457284')
    input('Press enter to continue')

    search_customer(first_name='IVAN')
    input('Press enter to continue')

    search_customer(email='sergey@')
    input('Press enter to continue')

    search_customer(first_name='Donald', last_name= 'Trump')
    print('Работа завершена')

