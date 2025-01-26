import psycopg2


conn = psycopg2.connect(database="study_db", user="postgres", password="qwerty")

def create_tables():
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
        conn.commit()
        print('Таблицы созданы')

def add_customer(first_name, last_name, email, phone=None):
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
        conn.commit()
        print('Клиент занесён в базу')

def add_new_phone(person_name_id, phone_number):
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO phone_number(person_name_id, number)
        VALUES(%s, %s);
        """, (person_name_id, phone_number))
        conn.commit()
        print('Номер телефона добавлен')

def change_data(person_name_id, first_name, last_name, email_id, email, phone_id, phone=None):
    with conn.cursor() as cur:
        cur.execute("""
        UPDATE person_name SET first_name=%s, last_name=%s WHERE id=%s;
        """, (first_name, last_name, person_name_id))

        cur.execute("""
        UPDATE phone_number SET number=%s WHERE id=%s;
        """, (phone, phone_id))

        cur.execute("""
        UPDATE email SET email=%s WHERE id=%s;
        """, (email, email_id))
        conn.commit()
        if cur.rowcount > 0:
            print('Данные о клиенте успешно изменены')
        else:
            print('Ошибка. Данные не заменены. Возможно клиент отсутствует в базе')

def dell_phone(phone_id):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM phone_number WHERE id=%s;
        """, (phone_id, ))
        conn.commit()
        if cur.rowcount > 0:
            print('Номер телефона успешно удалён')
        else:
            print('Ошибка. Данные не изменены. Возможно номер телефона отсутствует в базе')

def delete_customer(person_name_id):
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
        conn.commit()
        if cur.rowcount > 0:
            print('Данные о клиенте успешно удалены')
        else:
            print('Ошибка. Запись отсутствует в базе')

def search_customer(search_request: str):
    if search_request.isdigit():
        with conn.cursor() as cur:
            cur.execute("""
            SELECT pn.first_name, pn.last_name, p.number, e.email
            FROM person_name pn
            RIGHT JOIN phone_number p
            ON pn.id = p.person_name_id
            RIGHT JOIN email e
            ON pn.id = e.person_name_id
            WHERE e.person_name_id = 
            (
                SELECT p.person_name_id
                FROM phone_number p
                WHERE CAST(p.number AS TEXT) = %s
            ) 
            OR p.person_name_id =
            (
                SELECT p.person_name_id
                FROM phone_number p
                WHERE CAST(p.number AS TEXT) = %s
            );
            """, (search_request, search_request))
            result = cur.fetchall()
    if '@' in search_request:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT pn.first_name, pn.last_name, p.number, e.email
            FROM person_name pn
            RIGHT JOIN phone_number p
            ON pn.id = p.person_name_id
            RIGHT JOIN email e
            ON pn.id = e.person_name_id
            WHERE e.person_name_id = 
            (
                SELECT e.person_name_id
                FROM email e
                WHERE e.email = %s
            ) 
            OR p.person_name_id =
            (
                SELECT e.person_name_id
                FROM email e
                WHERE e.email = %s
            );
            """, (search_request, search_request))
            result = cur.fetchall()
    if len(search_request.split()) == 1 and (not
        search_request.isdigit()) and ("@" not in search_request):
        with conn.cursor() as cur:
            cur.execute("""
            SELECT pn.first_name, pn.last_name, p.number, e.email
            FROM person_name pn
            RIGHT JOIN phone_number p
            ON pn.id = p.person_name_id
            RIGHT JOIN email e
            ON pn.id = e.person_name_id
            WHERE pn.first_name ILIKE %s ESCAPE '';
            """, (search_request + '%', ))
            result = cur.fetchall()
    if len(search_request.split()) > 1:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT pn.first_name, pn.last_name, p.number, e.email
            FROM person_name pn
            RIGHT JOIN phone_number p
            ON pn.id = p.person_name_id
            RIGHT JOIN email e
            ON pn.id = e.person_name_id
            WHERE LOWER(pn.first_name) = %s
            AND pn.last_name ILIKE %s ESCAPE '';
            """, (search_request.split()[0].lower(),
                  search_request.split(' ', 1)[1] + '%'))
            result = cur.fetchall()
# print
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

# ===Test functions===
try:
    create_tables()
except psycopg2.errors.DuplicateTable:
    conn.rollback()
    print('Ошибка создания таблиц')
input('Press enter to continue')

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

try:
    add_new_phone(1, 85557771212)
except psycopg2.errors.UniqueViolation:
    conn.rollback()
    print('Ошибка добавления. Номер телефона уже существуют')
input('Press enter to continue')

change_data(3, 'Mikhail', 'Angalev',
        3, 'mikhail@aa.com', 3, 1533325562)
input('Press enter to continue')

dell_phone(2)
input('Press enter to continue')

delete_customer(2)
input('Press enter to continue')

search_customer('olga')
input('Press enter to continue')

search_customer('mikhail A')
input('Press enter to continue')

search_customer('89083457284')
input('Press enter to continue')

search_customer('mikhail@aa.com')
input('Press enter to continue')

search_customer('Donald Trump')
conn.close()
print('Работа завершена')

