import requests
import json
import os
import argparse
from neo4j import GraphDatabase
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_URL = "https://api.vk.com/method/"
API_VERSION = "5.131"
ACCESS_TOKEN = os.getenv("VK_ACCESS_TOKEN")
timeout = 5

# Подключение к Neo4j
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "12345678")

# Инициализация драйвера для подключения к Neo4j
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def get_numeric_user_id(user_name):
    url = f"{BASE_URL}users.get"
    params = {
        "user_ids": user_name,
        "access_token": ACCESS_TOKEN,
        "v": API_VERSION
    }
    response = requests.get(url, params=params).json()
    if response.get("response"):
        return response["response"][0]["id"]
    else:
        print("Error fetching user ID:", response)
        return None
def get_user_info(user_id, timeout=5):
    url = f"{BASE_URL}users.get"
    params = {
        "user_ids": user_id,
        "fields": "screen_name,sex,home_town,city",
        "access_token": ACCESS_TOKEN,
        "v": API_VERSION
    }
    # Проверка на успешный ответ от API
    try:
        response = requests.get(url, params=params, timeout=timeout).json()
        if "error" in response:
            error_code = response["error"].get("error_code")
            if error_code == 30:  # Код ошибки 30 - приватный профиль
                logger.warning(f"Профиль пользователя {user_id} приватный. Подписчики недоступны.")
                return []  # Возвращаем пустой список, если профиль приватный
            else:
                logger.warning(f"Ошибка при получении подписчиков пользователя {user_id}: {response}")
                return []

        elif response.get("response"):
            return response["response"][0]
        else:
            logger.warning(f"Не удалось получить информацию о пользователе {user_id}: {response}")
            return None
    except requests.exceptions.Timeout:
        logger.error(f"Тайм-аут при получении информации о пользователе {user_id}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка сети при получении информации о пользователе {user_id}: {e}")
        return None
def save_user_to_neo4j(user_id, user_name, screen_name=None, sex=None, home_town=None, city_title=None):
    with driver.session() as session:
        session.run(
            """
            MERGE (u:User {id: $user_id})
            SET u.name = $user_name,
                u.screen_name = $screen_name,
                u.sex = $sex,
                u.home_town = $home_town,
                u.city_title = $city_title
            """,
            user_id=user_id,
            user_name=user_name,
            screen_name=screen_name,
            sex=sex,
            home_town=home_town,
            city_title=city_title
        )
def save_group_to_neo4j(group_id, name, screen_name=None):
    with driver.session() as session:
        session.run(
            """
            MERGE (g:Group {id: $group_id})
            SET g.name = $name,
                g.screen_name = $screen_name
            """,
            group_id=group_id,
            name=name,
            screen_name=screen_name
        )
def save_subscription_relationship(user_id, subscription_id, subscription_name, is_subscription=False, is_group=False):
    with driver.session() as session:
        # для подписок (группы/люди) связь SUBSCRIBED_TO
        if is_subscription:
            if is_group:
                session.run(
                    """
                    MATCH (u:User {id: $user_id})
                    MERGE (g:Group {id: $subscription_id, name: $subscription_name})
                    MERGE (u)-[:SUBSCRIBED_TO]->(g)
                    """,
                    user_id=user_id, subscription_id=subscription_id, subscription_name=subscription_name
                )
            else:
                session.run(
                    """
                    MATCH (u:User {id: $user_id})
                    MERGE (s:User {id: $subscription_id, name: $subscription_name})
                    MERGE (u)-[:SUBSCRIBED_TO]->(s)
                    """,
                    user_id=user_id, subscription_id=subscription_id, subscription_name=subscription_name
                )
        # для подписчиков связь FOLLOW
        else:
            session.run(
                """
                MATCH (u:User {id: $user_id})
                MERGE (s:User {id: $subscription_id, name: $subscription_name})
                MERGE (u)-[:FOLLOW]->(s)
                """,
                user_id=user_id, subscription_id=subscription_id, subscription_name=subscription_name
            )

def get_subscriptions(user_id):
    url = f"{BASE_URL}users.getSubscriptions"
    params = {
        "user_id": user_id,
        "extended": 1,
        "count": 100,
        "access_token": ACCESS_TOKEN,
        "v": API_VERSION
    }
    try:
        response = requests.get(url, params=params).json()
        if response.get("response"):
            for subscribtion in response['response']['items']:
                # среди подписок могут быть как пользователи, так и группы
                # пользователи обрабатываются иначе (для них больше полей)
                if subscribtion.get('type') == 'profile':
                    user_info = get_user_info(subscribtion.get('id'))
                    full_name = f"{user_info.get('first_name', '')} {user_info.get('last_name', '')}"
                    save_user_to_neo4j(subscribtion.get('id'),
                                       full_name,
                                       user_info.get("screen_name"),
                                       user_info.get("sex"),
                                       user_info.get("home_town"),
                                       user_info.get("city", {}).get("title"))
                    save_subscription_relationship(user_id, subscribtion.get('id'), full_name, is_subscription=True, is_group=False)
                else:
                    save_group_to_neo4j(subscribtion.get('id'),
                                        subscribtion.get('name'),
                                        subscribtion.get('screen_name'))
                    save_subscription_relationship(user_id, subscribtion.get('id'), subscribtion.get('name', ''), is_subscription=True, is_group=True)
                logger.info(f"Добавлена подписка: {subscribtion}")

        else:
            logger.warning(f"Не удалось получить информацию о пользователе {user_id}: {response}")
            return None
    except requests.exceptions.Timeout:
        logger.error(f"Тайм-аут при получении информации о пользователе {user_id}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка сети при получении информации о пользователе {user_id}: {e}")
        return None


def get_followers(user_id):
    followers = []
    url = f"{BASE_URL}users.getFollowers"
    params = {
        "user_id": user_id,
        "count": 100,
        "access_token": ACCESS_TOKEN,
        "v": API_VERSION
    }
    try:
        response = requests.get(url, params=params, timeout=timeout).json()
        if "error" in response:
            error_code = response["error"].get("error_code")
            if error_code == 30:  # Код ошибки 30 - приватный профиль
                logger.warning(f"Профиль пользователя {user_id} приватный. Подписчики недоступны.")
                return []  # Возвращаем пустой список, если профиль приватный
            else:
                logger.warning(f"Ошибка при получении подписчиков пользователя {user_id}: {response}")
                return []

        elif response.get("response"):
            for follower_id in response['response']['items']: # список id подписчиков
                user_info = get_user_info(follower_id) # получение информации о пользователе по его id
                # print(user_info)
                if user_info:
                    full_name = f"{user_info.get('first_name', '')} {user_info.get('last_name', '')}"
                    save_user_to_neo4j(follower_id,
                                       full_name,
                                       user_info.get("screen_name"),
                                       user_info.get("sex"),
                                       user_info.get("home_town"),
                                       user_info.get("city", {}).get("title"))
                    save_subscription_relationship(user_id, follower_id, full_name)
                # session.write_transaction(create_user, user_info)
                logger.info(f"Добавлен пользователь: {user_info}")
                # session.write_transaction(create_follower_relationship, user_id, follower['id'])
                followers.append(follower_id)
        else:
            logger.warning(f"Не удалось получить информацию о пользователе {user_id}: {response}")
            return None
    except requests.exceptions.Timeout:
        logger.error(f"Тайм-аут при получении информации о пользователе {user_id}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка сети при получении информации о пользователе {user_id}: {e}")
        return None

    return followers
def process_user_and_followers(user_id, depth=0):
    if depth < 2:
        print('Текущий уровень: ', depth)
        # get_subscriptions(user_id)
        user_info = get_user_info(user_id)
        full_name = f"{user_info.get('first_name', '')} {user_info.get('last_name', '')}"
        save_user_to_neo4j(user_id,
                           full_name,
                           user_info.get("screen_name"),
                           user_info.get("sex"),
                           user_info.get("home_town"),
                           user_info.get("city", {}).get("title"))


        followers = get_followers(user_id)
        get_subscriptions(user_id)

        for follower_id in followers:
            process_user_and_followers(follower_id, depth + 1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Получение информации о пользователе ВКонтакте и сохранение в Neo4j")
    # parser.add_argument("--user_id", type=str, default="olegan_west", help="ID пользователя ВКонтакте")
    # parser.add_argument("--user_id", type=str, default="besso_sonia", help="ID пользователя ВКонтакте")
    parser.add_argument("--user_id", type=str, default="yllwftc00", help="ID пользователя ВКонтакте")

    args = parser.parse_args()
    process_user_and_followers(get_numeric_user_id(args.user_id))