import aiohttp
import asyncio
import pandas as pd
import time
import logging


# Configure logging to provide detailed information about the scraping process
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

async def fetch_detail(session, queue, all_details):
    """
    Fetches details for a specific property identified by its ID from the Sreality website.

    Args:
        session: An aiohttp client session object used for making HTTP requests.
        queue: An asyncio queue used to manage the retrieval of property IDs.
        all_details: A list to store the details of all scraped properties.
    """
    while True:
        detail_id = await queue.get()
        if detail_id is None:  # Sentinel value to indicate completion
            break
        
        logging.info(f"Fetching details for property ID: {detail_id}")
        
        detail_url = f'https://www.sreality.cz/api/cs/v2/estates/{detail_id}'
        try:
            async with session.get(detail_url) as response:
                if response.status == 200:
                    json_info = await response.json()
                    
                    # Extract relevant property details from the JSON response
                    detail_data = {
                        'title': json_info.get('name', {}).get('value', ''),
                        'description': json_info.get('meta_description', ''),
                        'address': json_info.get('locality', {}).get('value', ''),
                        'price': json_info.get('price_czk', {}).get('value_raw', ''),
                        'seller_name': json_info.get('_embedded').get('seller', {}).get('user_name', ''),
                        'seller_email': json_info.get('_embedded').get('seller', {}).get('email', ''),
                        'seller_id': json_info.get('_embedded').get('seller', {}).get('user_id', ''),
                        'code_number': '',
                        'seller_phone': '',
                    }
                    
                    # Extract the phones list, with a default to an empty list if not found
                    phones_list = json_info.get('_embedded', {}).get('seller', {}).get('phones', [])
                    # Check if the phones list is not empty and then extract the 'code'
                    if phones_list:
                        detail_data['code_number'] = phones_list[0].get('code', '')
                        detail_data['seller_phone'] = phones_list[0].get('number', '')
                    else:
                        logging.debug(f"No phone numbers found for property ID: {detail_id}")
                        detail_data['code_number'] = ''
                        detail_data['seller_phone'] = ''

                     # Extract additional details from 'items' list in the JSON response
                    for item in json_info['items']:
                        name = item.get('name')
                        value = item.get('value')
                        detail_data = {**detail_data, **{name: value}}
                        # detail_data[name] = value
                    
                    all_details.append(detail_data)
                    logging.info(f'Successfully fetched detail {detail_id}')
                else:
                    logging.error(f"Error fetching detail {detail_id}: {response.status}")
        except aiohttp.ClientError as e:
            logging.exception(f"Client error fetching detail {detail_id}: {e}")
        finally:
            queue.task_done()

async def fetch_page(session, page_number, queue):
    """
    Fetches a specific page from the Sreality search results and extracts property IDs for details retrieval.

    Args:
        session: An aiohttp client session object used for making HTTP requests.
        page_number: The page number to fetch from the search results.
        queue: An asyncio queue used to store property IDs for detail retrieval.

    Returns:
        True if property IDs were found on the fetched page, False otherwise.
    """
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'sk-SK,sk;q=0.9,cs;q=0.8,en-US;q=0.7,en;q=0.6',
        'referer': 'https://www.sreality.cz/hledani/prodej/byty?no_shares=1&bez-aukce=1',
        'sec-ch-ua': '"Google"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    }
    page_url = f'https://www.sreality.cz/api/cs/v2/estates?category_main_cb=1&category_type_cb=1&no_shares=1&bez-aukce=1&page={page_number}'
    try:
        async with session.get(page_url, headers=headers) as response:
            if response.status == 200:
                json_data = await response.json()
                start_point = json_data['_embedded']['estates']
                if start_point:
                    for estate in start_point:
                        detail_id = estate.get('hash_id', '')
                        if detail_id:
                            await queue.put(detail_id)
                    logging.info(f'Successfully fetched page {page_number}')
                    return True
                else:
                    logging.warning(f'No estates found on page {page_number}')
                    return False
            else:
                logging.error(f"Error fetching page {page_number}: {response.status}")
                return False
    except aiohttp.ClientError as e:
        logging.exception(f"Client error fetching page {page_number}: {e}")
        return False

async def main():
    """
    The main function that orchestrates the scraping process.

    This function starts an aiohttp client session, creates a queue for managing property IDs,
    and a list to store scraped property details. It then iterates through web pages,
    extracting property IDs and fetching details for each property concurrently.
    Finally, it converts the scraped data into a Pandas DataFrame.
    """
    async with aiohttp.ClientSession() as session:
        queue = asyncio.Queue()
        all_details = []
        page_number = 800  # Start from page 1
        detail_tasks = []
        
        start_time = time.time()
        # Continue looping as long as 'proced' is True
        logging.info(f"Scraping process started at {time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        while True:
            logging.info(f'Currently scraping page: {page_number}')
            page_fetched = await fetch_page(session, page_number, queue)
            if not page_fetched:
                logging.warning(f"No details found on page {page_number}, skipping.")
                break

            # Start detail tasks if they haven't been started yet
            if not detail_tasks:
                for _ in range(50):  # Adjust the number of concurrent tasks as needed
                    task = asyncio.create_task(fetch_detail(session, queue, all_details))
                    detail_tasks.append(task)

            page_number += 1
            
        # Signal the detail tasks to exit
        for _ in detail_tasks:
            await queue.put(None)

        # Wait for all detail tasks to complete
        await asyncio.gather(*detail_tasks)

        # Convert all_details to a DataFrame and print it (optional)
        try:
            df = pd.DataFrame(all_details)
            # df.to_csv('scrape sreality/sreality_output_data.csv', index=False, encoding='utf-8-sig')
            logging.info(f"Successfully created DataFrame with scraped data.")
            print(df)
        except Exception as e:
            logging.error(f"Error creating DataFrame: {e}")

        # Get the current time after the completion of the operation
        end_time = time.time()

        # Log information about the duration of the operation
        logging.info(f"This operation took: {end_time - start_time} seconds")

        # Calculate the total duration of the operation in seconds
        duration_seconds = end_time - start_time

        # Convert the total duration to minutes and seconds
        minutes, seconds = divmod(duration_seconds, 60)
        duration_in_minutes = int(minutes)  # Total number of minutes
        duration_in_seconds = int(seconds)  # Total number of seconds

        # Log information about the duration of the operation in minutes and seconds
        logging.info(f"This operation took: {duration_in_minutes} minutes {duration_in_seconds} seconds")

# Run the main function
if __name__ == '__main__':
    asyncio.run(main())
