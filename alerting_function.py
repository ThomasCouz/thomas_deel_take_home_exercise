from datetime import datetime, timedelta
from dotenv import dotenv_values
import snowflake.connector
import pandas as pd
import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import sys

env_file_path = '.env'
env_variables = dict(dotenv_values(env_file_path))


def extract_yesterday_data(table_name: str, start_date: str, end_date: str, output_filename: str) -> str:
    """
    Select all lines in snowflake table within a given date range and store results in csv file

    :param table_name: full name of the snowflake table (in format database.schema.table)
    :param start_date: date from which to select lines (inclusive)
    :param end_date: date to which to select lines (inclusive)
    :param output_filename: name of the csv file in which to store the results
    :return: output_filename
    """
    conn = snowflake.connector.connect(
        user=env_variables.get('USER'),
        password=env_variables.get('PASSWORD'),
        account=env_variables.get('ACCOUNT'),
        role=env_variables.get('ROLE'),
        database=env_variables.get('DATABASE'),
        warehouse=env_variables.get('WAREHOUSE'),
    )

    sql_query = f"SELECT * " \
                f"FROM {table_name} " \
                f"WHERE DATE(date_day) between '{start_date}' and '{end_date}'"

    try:
        df = pd.read_sql(sql_query, conn)
        df.to_csv(output_filename, index=False)
        print(f"Query executed and results saved to {output_filename}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()

    return output_filename


def send_csv_to_slack(file_path: str, slack_token: str, channel_id: str, comment: str) -> None:
    """
    Sends a message with the CSV file to a Slack channel.

    Parameters:
    :param file_path: The path to the CSV file.
    :param slack_token: The Slack API token.
    :param channel_id: The Slack channel ID to send the message to.
    """
    client = WebClient(token=slack_token)

    try:
        response = client.files_upload_v2(
            channel=channel_id,
            file=file_path,
            title=os.path.basename(file_path),
            initial_comment=comment
        )
        assert response["file"], "Failed to upload the file."
        print(f"File uploaded successfully to channel {channel_id}")

    except SlackApiError as e:
        print(f"Error uploading file to Slack: {e.response['error']}")


def alert_customer_financial_balance_changes(start_date: str = None, end_date: str = None) -> None:
    """
    Push csv in C074H3GFB97 slack channel with the list of organizations that have a financial balance that
    changes by more than 50% vs previous day

    Parameters:
    :param start_date: optional, start of the date range considered for daily financial balances (inclusive).
    Default value to yesterday if omitted
    :param end_date: optional, end of the date range considered for daily financial balances (inclusive).
    Default value to yesterday if omitted
    """

    table_name = 'dev_thomas.deel.fct_alerts_organization_financial_balance_moves_50_pct'
    slack_comment = "List of new occurences of organization's financial balance changes by more than 50%: "
    csv_filename = 'tmp_output.csv'

    if not start_date:
        start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    if not end_date:
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    extract_yesterday_data(table_name=table_name, start_date=start_date, end_date=end_date,
                           output_filename=csv_filename)

    send_csv_to_slack(file_path=csv_filename, slack_token=env_variables.get('SLACK_API_TOKEN'),
                      channel_id=env_variables.get('SLACK_CHANNEL_ID'), comment=slack_comment)

    os.system(f"rm {csv_filename}")


if __name__ == "__main__":
    start_date = None
    end_date = None
    if len(sys.argv) >= 2:
        start_date = sys.argv[1]
    if len(sys.argv) >= 3:
        start_date = sys.argv[2]
    alert_customer_financial_balance_changes(start_date=start_date, end_date=end_date)
