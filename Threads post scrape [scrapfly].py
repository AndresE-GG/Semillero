import json
from typing import Dict

import jmespath
from nested_lookup import nested_lookup
from scrapfly import ScrapflyClient, ScrapeConfig

# SCRAPFLY = ScrapflyClient(key = "scp-live-18fc1b0a98404d79b45d4d7801f6859a")
SCRAPFLY = ScrapflyClient(key = "scp-test-b37221dad02c489a9801702b8a4b3538")

def parse_thread(data: Dict) -> Dict:
    """Parse Twitter tweet JSON dataset for the most important fields"""
    result = jmespath.search(
        """{
        text: post.caption.text
    }""",
        data,
    )
    return result

async def scrape_thread(url: str) -> dict:
    """Scrape Threads post and replies from a given URL"""
    _xhr_calls = []
    result = await SCRAPFLY.async_scrape(
        ScrapeConfig(
            url,
            asp=True,  # enables scraper blocking bypass if any
            country="US",  # use US IP address as threads is only available in select countries
        )
    )
    hidden_datasets = result.selector.css(
        'script[type="application/json"][data-sjs]::text'
    ).getall()
    # find datasets that contain threads data
    for hidden_dataset in hidden_datasets:
        # skip loading datasets that clearly don't contain threads data
        if '"ScheduledServerJS"' not in hidden_dataset:
            continue
        if "thread_items" not in hidden_dataset:
            continue
        data = json.loads(hidden_dataset)
        # datasets are heavily nested, use nested_lookup to find
        # the thread_items key for thread data
        thread_items = nested_lookup("thread_items", data)
        if not thread_items:
            continue
        # use our jmespath parser to reduce the dataset to the most important fields
        threads = [parse_thread(t) for thread in thread_items for t in thread]
        return {
            "thread": threads[0],
            "replies": threads[1:],
        }
    raise ValueError("could not find thread data in page")


# Example use:
if __name__ == "__main__":
    import asyncio
    a = asyncio.run(scrape_thread("Link"))