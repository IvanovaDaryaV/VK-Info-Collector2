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

# Подключение к Neo4j
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "12345678")

# Инициализация драйвера для подключения к Neo4j
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
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

def save_subscription_relationship(user_id, subscription_id, subscription_name, is_group=False):
    with driver.session() as session:
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
                MERGE (u)-[:FOLLOW]->(s)
                """,
                user_id=user_id, subscription_id=subscription_id, subscription_name=subscription_name
            )


def get_user_info(user_id, timeout=5):
    url = f"{BASE_URL}users.get"
    params = {
        "user_ids": user_id,
        "fields": "screen_name,sex,home_town,city",
        "access_token": ACCESS_TOKEN,
        "v": API_VERSION
    }
    # response = requests.get(url, params=params).json()
    # return response.get("response", [])[0] if response.get("response") else None

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

def get_group_info(group_id, timeout=5):
    url = f"{BASE_URL}groups.getById"
    params = {
        "group_id": group_id,
        "fields": "id,name,screen_name",
        "access_token": ACCESS_TOKEN,
        "v": API_VERSION
    }

    # Проверка на успешный ответ от API
    try:
        response = requests.get(url, params=params, timeout=timeout).json()
        if response.get("response"):
            group_info = response["response"][0]
            return {
                "id": group_info.get("id"),
                "name": group_info.get("name"),
                "screen_name": group_info.get("screen_name")
            }
        else:
            logger.warning(f"Не удалось получить информацию о группе {group_id}: {response}")
            return None
    except requests.exceptions.Timeout:
        logger.error(f"Тайм-аут при получении информации о группе {group_id}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка сети при получении информации о группе {group_id}: {e}")
        return None
def get_followers(user_id, timeout=5):
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
            return response.get("response", {}).get("items", [])
        else:
            logger.warning(f"Не удалось получить информацию о пользователе {user_id}: {response}")
            return None
    except requests.exceptions.Timeout:
        logger.error(f"Тайм-аут при получении информации о пользователе {user_id}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка сети при получении информации о пользователе {user_id}: {e}")
        return None

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
            return response.get("response", {}).get("items", [])
        else:
            logger.warning(f"Не удалось получить информацию о пользователе {user_id}: {response}")
            return None
    except requests.exceptions.Timeout:
        logger.error(f"Тайм-аут при получении информации о пользователе {user_id}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка сети при получении информации о пользователе {user_id}: {e}")
        return None
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

def main(user_id):
    user_name = user_id
    user_id = get_numeric_user_id(user_name)
    if not user_id:
        print("Не удалось получить числовой ID пользователя.")
        return

    user_info = get_user_info(user_id)
    print("Получаем информацию об основном пользователе...")
    if user_info:
        user_full_name = f"{user_info.get('first_name', '')} {user_info.get('last_name', '')}"
        screen_name = user_info.get("screen_name")
        sex = user_info.get("sex")
        home_town = user_info.get("home_town")
        city_title = user_info.get("city", {}).get("title")

        save_user_to_neo4j(user_id, user_full_name, screen_name, sex, home_town, city_title)
        followers = get_followers(user_id)
        subscriptions = get_subscriptions(user_id)

        # получение информации о группах, на которые человек подписан
        i = 0
        for sub in subscriptions:
            print(f"Получаем группы основного человека: {i}")
            i += 1
            group_screen_name = sub["screen_name"]
            group_name = sub["name"]
            sub_group_id = sub["id"]

            save_group_to_neo4j(
                sub_group_id,
                group_name,
                group_screen_name
            )
            save_subscription_relationship(user_id, sub_group_id, group_name, is_group=True)

        i = 0
        # получение информации о подписчиках
        for sub_id in followers:
            print(f"Получаем подписчиков: {i}")
            i += 1
            follower_info = get_user_info(sub_id)  # Получаем полные данные


            follower_full_name = f"{follower_info.get('first_name', '')} {follower_info.get('last_name', '')}"
            follower_screen_name = follower_info.get("screen_name")
            follower_sex = follower_info.get("sex")
            follower_home_town = follower_info.get("home_town")
            follower_city_title = follower_info.get("city", {}).get("title")

            save_user_to_neo4j(
                sub_id,
                follower_full_name,
                follower_screen_name,
                follower_sex,
                follower_home_town,
                follower_city_title
            )
            #связь подписчика с основным человеком
            save_subscription_relationship(user_id, sub_id, follower_full_name)

            #для каждого подписчика получаем список его подписок/подписчиков
            sub_followers = get_followers(sub_id)
            sub_subscriptions = get_subscriptions(sub_id)

            # получение информации о группах, на которые подписчик подписан
            j = 0
            for sub in sub_subscriptions:
                print(f"Получаем группы подписчика {i}: {j}")
                j += 1
                # print(sub_group)
                if sub["type"] == "page":
                    group_screen_name = sub["screen_name"]
                    group_name = sub["name"]
                    sub_group_id = sub["id"]

                    save_group_to_neo4j(
                        sub_group_id,
                        group_name,
                        group_screen_name
                    )
                    save_subscription_relationship(sub_id, sub_group_id, group_name, is_group=True)
                else:
                    # logger.warning(f"Пропущен элемент, так как это не группа: {sub_group}")
                    follower_info = get_user_info(sub['id'])
                    follower_full_name = f"{follower_info.get('first_name', '')} {follower_info.get('last_name', '')}"
                    follower_screen_name = follower_info.get("screen_name")
                    follower_sex = follower_info.get("sex")
                    follower_home_town = follower_info.get("home_town")
                    follower_city_title = follower_info.get("city", {}).get("title")

                    save_user_to_neo4j(
                        sub['id'],
                        follower_full_name,
                        follower_screen_name,
                        follower_sex,
                        follower_home_town,
                        follower_city_title
                    )
                    save_subscription_relationship(sub_id, sub['id'], follower_full_name)
            j = 0
            for follower_id in sub_followers:
                print(f"Получаем подписчиков подписчика {i}: {j}")
                j += 1
                follower_info = get_user_info(follower_id)  # Получаем полные данные

                if follower_info:
                    follower_full_name = f"{follower_info.get('first_name', '')} {follower_info.get('last_name', '')}"
                    follower_screen_name = follower_info.get("screen_name")
                    follower_sex = follower_info.get("sex")
                    follower_home_town = follower_info.get("home_town")
                    follower_city_title = follower_info.get("city", {}).get("title")

                    save_user_to_neo4j(
                        follower_id,
                        follower_full_name,
                        follower_screen_name,
                        follower_sex,
                        follower_home_town,
                        follower_city_title
                    )
                    save_subscription_relationship(sub_id, follower_id, follower_full_name)

                    # sub_sub_subscriptions = get_subscriptions(follower_id)
                    # sub_sub_subscriptions = get_subscriptions(sub_id)

                    # получение информации о группах, на которые подписаны подписчики
                    # k = 0
                    # for sub_group in sub_sub_subscriptions:
                    #     print(f"Получаем группы подписчика подписчика {i}: {k}")
                    #     k += 1
                    #     if sub_group["type"] == "page":
                    #         group_screen_name = sub_group["screen_name"]
                    #         group_name = sub_group["name"]
                    #         sub_group_id = sub_group["id"]
                    #
                    #         save_group_to_neo4j(
                    #             sub_group_id,
                    #             group_name,
                    #             group_screen_name
                    #         )
                    #         save_subscription_relationship(user_id, sub_group_id, group_name, is_group=True)
                    #     else:
                    #         logger.warning(f"Пропущен элемент, так как это не группа: {sub_group}")
                    # follower_name = f"{follower_info.get('first_name', '')} {follower_info.get('last_name', '')}" if follower_info else ""
                    # save_user_to_neo4j(follower_id, follower_name)
                    # save_subscription_relationship(sub_id, follower_id, follower_name)

        print(f"Информация о подписках и подписчиках на два уровня сохранена в Neo4j.")
    else:
        print("Не удалось получить информацию о пользователе.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Получение информации о пользователе ВКонтакте и сохранение в Neo4j")
    # parser.add_argument("--user_id", type=str, default="olegan_west", help="ID пользователя ВКонтакте")
    parser.add_argument("--user_id", type=str, default="besso_sonia", help="ID пользователя ВКонтакте")

    args = parser.parse_args()
    main(args.user_id)
