import asyncio
import json
import os
import time
from playwright.async_api import async_playwright

CITY = "kanpur"
BASE_PATH = "./carwale-urls/"
FILE_NAME = f"urls_{CITY}.json"
OUTPUT_PATH = f"./output/{CITY}-data.jsonl"
PROGRESS_PATH = f"./output/{CITY}-progress.json"
URL_PATH = BASE_PATH + FILE_NAME
WEB_BASE_URL = "https://www.carwale.com"

CONCURRENCY = 5
RATE_LIMIT_DELAY = 0.3
BLOCKED_RESOURCES = ["image", "media", "font", "stylesheet"]


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_progress(path):
    if not os.path.exists(path):
        return set()
    with open(path, "r", encoding="utf-8") as f:
        return set(json.load(f))


def save_progress(done_urls, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(list(done_urls), f, ensure_ascii=False)


async def extract_car_data(page):
    return await page.evaluate("""
    () => {
        const result = {};
        const t = el => el ? el.textContent.trim().replace(/\\s+/g, ' ') : '';
        const a = (el, attr) => el ? el.getAttribute(attr) || '' : '';

        result['title'] = t(document.querySelector('h1.o-j7'));

        const priceDiv = document.querySelector('div.o-j0');
        result['new_car_price'] = priceDiv ? t(priceDiv.querySelector('div')) : '';

        result['car_img_url'] = a(document.querySelector('img.B6hlWy'), 'src');

        const overviews = Array.from(document.querySelectorAll('div.o-hT'));
        const start = overviews.length > 2 ? 1 : 0;
        const end = overviews.length > 2 ? overviews.length - 1 : overviews.length;
        for (let i = start; i < end; i++) {
            const l = overviews[i].querySelector('.o-jK');
            const v = overviews[i].querySelector('div.o-f5');
            if (l && v) result['overview_' + t(l)] = t(v);
        }

        document.querySelectorAll('div.kgwwPb > div > ul > li').forEach(s => {
            const l = s.querySelector('.o-jK');
            const v = s.querySelector('.o-jJ');
            if (l && v) result['spec_' + t(l)] = t(v);
        });

        return result;
    }
    """)


async def worker(worker_id, queue, session, context, sem):
    page = await context.new_page()
    page.set_default_timeout(30000)

    while True:
        car_url = await queue.get()
        if car_url is None:
            queue.task_done()
            break

        if car_url in session["done_urls"]:
            queue.task_done()
            continue

        final_url = WEB_BASE_URL + car_url

        try:
            async with sem:
                await page.goto(final_url, wait_until="domcontentloaded", timeout=30000)
                car_data = await extract_car_data(page)

            async with session["lock"]:
                with open(OUTPUT_PATH, "a", encoding="utf-8") as f:
                    f.write(json.dumps(car_data, ensure_ascii=False) + "\n")
                session["done_urls"].add(car_url)
                session["count"] += 1
                print(f"[W{worker_id}] {session['count']}. {car_data.get('title', 'Unknown')}")

                if session["count"] % 20 == 0:
                    save_progress(session["done_urls"], PROGRESS_PATH)
                    print(f"[W{worker_id}] Checkpoint saved at {session['count']} cars")

            await asyncio.sleep(RATE_LIMIT_DELAY)

        except Exception as e:
            print(f"[W{worker_id}] Error: {final_url} -> {type(e).__name__}: {e}")
            async with session["lock"]:
                session["done_urls"].add(car_url)
            await asyncio.sleep(2)

        finally:
            queue.task_done()

    await page.close()


async def main():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    all_urls = load_json(URL_PATH)
    done_urls = load_progress(PROGRESS_PATH)
    remaining = [u for u in all_urls if u not in done_urls]

    print(f"Total URLs: {len(all_urls)}")
    print(f"Already done: {len(done_urls)}")
    print(f"Remaining: {len(remaining)}")

    if not remaining:
        print("All cars already scraped. Nothing to do.")
        return

    queue = asyncio.Queue()
    for url in remaining:
        await queue.put(url)

    sem = asyncio.Semaphore(CONCURRENCY)
    session = {
        "done_urls": done_urls,
        "count": len(done_urls),
        "lock": asyncio.Lock(),
    }

    start_time = time.time()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 720},
        )

        await context.route(
            "**/*",
            lambda route: (
                route.abort()
                if route.request.resource_type in BLOCKED_RESOURCES
                else route.continue_()
            ),
        )

        workers = []
        for i in range(CONCURRENCY):
            w = asyncio.create_task(worker(i, queue, session, context, sem))
            workers.append(w)

        for _ in range(CONCURRENCY):
            await queue.put(None)

        await asyncio.gather(*workers)

        elapsed = time.time() - start_time
        save_progress(session["done_urls"], PROGRESS_PATH)
        print(f"\nAll done! Scraped {session['count']} cars in {elapsed:.1f}s")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
