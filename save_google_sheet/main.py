import gspread
from google.oauth2.service_account import Credentials

from instagrapi import Client
from instagrapi.exceptions import LoginRequired, ChallengeRequired
from datetime import datetime, timedelta
import os

from concurrent.futures import ThreadPoolExecutor
import time
import random

from dotenv import load_dotenv


class GoogleSheetClient:
    def __init__(self, creds_file, scopes):
        self.creds = Credentials.from_service_account_file(creds_file, scopes=scopes)
        self.client = gspread.authorize(self.creds)

    def open_workbook(self, sheet_id):
        return self.client.open_by_key(sheet_id)


class WorksheetManager:
    def __init__(self, workbook, sheet_name):
        self.workbook = workbook
        self.sheet_name = sheet_name
        self.worksheet = self.get_or_create_worksheet()

    def get_or_create_worksheet(self):
        worksheet_list = [worksheet.title for worksheet in self.workbook.worksheets()]
        if self.sheet_name in worksheet_list:
            return self.workbook.worksheet(self.sheet_name)
        else:
            return self.workbook.add_worksheet(self.sheet_name, rows=1000, cols=10)

    def append_values(self, values):
        for row in values:
            self.worksheet.append_row(row)

    def format_header(self):
        self.worksheet.freeze(rows=1)
        self.worksheet.format("A1:C1", {"textFormat": {"bold": True}})

    def get_first_column_data(self):
        return self.worksheet.col_values(1)[:]

    def add_filter(self):
        # Calculate the range dynamically for all data (including headers)
        num_rows = self.worksheet.row_count
        num_cols = self.worksheet.col_count
        range_name = f"A1:{chr(64 + num_cols)}{num_rows}"  # Construct the range like "A1:C100"

        # Apply the filter to the calculated range
        self.worksheet.add_filter(range_name)


class ScraperInstagram:
    def __init__(self, username, password, session_file="./instagram_session.json"):
        self.username = username
        self.password = password
        self.session_file = session_file
        self.cl = Client()
        self.one_week_ago = datetime.now() - timedelta(days=7)
        self.two_hours_ago = datetime.now() - timedelta(hours=10)

    def login_with_session(self):
        try:
            self.cl.load_settings(self.session_file)
            self.cl.login(self.username, self.password)
        except FileNotFoundError:
            print("Session was not found. Auth...")
            self.cl.login(self.username, self.password)
            self.cl.dump_settings(self.session_file)
        except LoginRequired:
            if os.path.exists(self.session_file):
                os.remove(self.session_file)
            print("Session is not valid. Auth again...")
            self.cl.login(self.username, self.password)
            self.cl.dump_settings(self.session_file)
        except ChallengeRequired as e:
            print("Challenge detected. Please complete the verification.")

            if e.challenge.get("step_name") == "select_verify_method":
                print("Please select a verification method (e.g., email, SMS).")

            code = input("Enter the verification code sent to your email/phone: ")

            self.cl.challenge_complete(code)

            self.cl.dump_settings(self.session_file)
            print(f"Session saved to {self.session_file}.")
        except Exception as ex:
            print(f"An unexpected error occurred: {ex}")

    def scraping_process(self, target_username):
        time.sleep(random.uniform(2, 5))
        try:
                user_id = self.cl.user_id_from_username(target_username)
                print("-" * 60)
                print(f"User id @{target_username}: {user_id}")

                # take all media (posts + reels) and stories
                media = self.cl.user_medias(user_id, amount=50)
                stories = self.cl.user_stories(user_id)

                # filtration by the date
                recent_posts = self.filter_by_date(media, self.one_week_ago)
                recent_stories = self.filter_by_date(stories, self.two_hours_ago)

                # extend data with the parsed information
                posts_data = (self.parse_media_data(recent_posts, "media", user_id, target_username))
                stories_data = (self.parse_media_data(recent_stories, "story", user_id, target_username))

                return posts_data, stories_data
        except Exception as e:
                print(f"Error during parsing the @{target_username}: {e}")
        return [], []

    @staticmethod
    def parse_media_data(data, type_media, user_id, target_username):
        data_list = []
        if data:
            for i, el in enumerate(data, start=1):
                url = None
                if type_media == "media":
                    url = f"https://www.instagram.com/p/{el.code}/" if el.code else "Ссылка недоступна"
                elif type_media == "story":
                    url = f"https://www.instagram.com/stories/{target_username}/{el.pk}/"

                formatted_date = el.taken_at.strftime('%Y-%m-%d-%H-%M')

                print(f"\n******************** {type_media} #{i} ********************")
                print(f"ID of {type_media}: {el.id}")
                print(f"Date of publication: {formatted_date}")
                print(f"URL of {type_media}: {url}")
                # print(f"Type: {post.media_type}")
                # print(f"Description: {post.caption_text}")
                # print(f"URL of the content: {post.thumbnail_url if post.media_type == 1 else post.video_url}")
                # print(f"Number of likes: {post.like_count}")
                # print(f"Number of comments: {post.comment_count}")

                current_data = {
                    "user_id": user_id,
                    "username": target_username,
                    "link": url,
                    "datetime": formatted_date,
                }
                data_list.append(current_data)
        else:
            print(f"User @{target_username} does not have {type_media} data.")

        return data_list

    @staticmethod
    def filter_by_date(data, timedelta):
        filtered_data = [
            el for el in data
            if datetime.fromtimestamp(el.taken_at.timestamp()) > timedelta
        ]
        return filtered_data


# Usage Example
if __name__ == "__main__":
    creds_file = "credentials.json"
    sheet_id = "1n361E435E0EJdMFSYmZc5U1_W6q_McZkEOUqOzv8Cmo"
    sheet_name_1 = "UserList"
    sheet_name_2 = "Posts"
    sheet_name_3 = "Stories"
    scopes = None

    load_dotenv()
    username = os.getenv("INSTAGRAM_USERNAME")
    password = os.getenv("INSTAGRAM_PASSWORD")
    session_file = "./instagram_session.json"


    if scopes is None:
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
    sheet_client = GoogleSheetClient(creds_file, scopes)
    workbook = sheet_client.open_workbook(sheet_id)

    worksheet_manager_1 = WorksheetManager(workbook, sheet_name_1)
    worksheet_manager_2 = WorksheetManager(workbook, sheet_name_2)
    worksheet_manager_3 = WorksheetManager(workbook, sheet_name_3)

    target_username_list = worksheet_manager_1.get_first_column_data()
    print("Target Username List length :", len(target_username_list))

    scraper = ScraperInstagram(username, password, session_file)
    scraper.login_with_session()

    with ThreadPoolExecutor(max_workers=5) as executor:
        results = executor.map(scraper.scraping_process, target_username_list)

    posts_data = []
    stories_data = []
    for posts, stories in results:
        posts_data.extend(posts)
        stories_data.extend(stories)

    post_values_list = [list(post.values()) for post in posts_data]
    stories_values_list = [list(post.values()) for post in stories_data]

    worksheet_manager_2.append_values(post_values_list)
    worksheet_manager_3.append_values(stories_values_list)

    worksheet_manager_2.add_filter()
    worksheet_manager_3.add_filter()

    worksheet_manager_1.format_header()
    worksheet_manager_2.format_header()
    worksheet_manager_3.format_header()


