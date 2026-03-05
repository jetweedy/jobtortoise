from playwright.async_api import async_playwright
import asyncio

async def ainput(prompt: str = "") -> str:
    return await asyncio.to_thread(input, prompt)

async def trySchool(page, school_edu: str) -> bool:
    school_edu = school_edu.strip()
    if not school_edu:
        return False

    url = f"https://jobs.{school_edu}"
    print("\n=== ", school_edu, "===")
    print("start:", url)

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=15000)
        await page.wait_for_timeout(1500)

        # rewrite to /postings/search based on whatever hostname you ended up at
        did_nav = await page.evaluate("""
        () => {
            const u = new URL(window.location.href);
            const m = u.hostname.match(/(.+\\.edu)$/);
            if (!m) return false;
            window.location.href = `https://${m[1]}/postings/search`;
            return true;
        }
        """)

        if did_nav:
            await page.wait_for_load_state("domcontentloaded")
        else:
            print("No *.edu hostname match after redirect; staying on current page:", page.url)

        print("now:", page.url)

        # HARD STOP: you must answer before continuing
        yn = await ainput("Add to list? (y/N) [or just Enter to skip]: ")
        return yn.strip().lower() == "y"

    except Exception as e:
        print(f"[{school_edu}] error: {e}")
        # HARD STOP even on error, so it can’t “fly through”
        await ainput("Error occurred. Press Enter to continue to next school...")
        return False

async def go():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        with open("schools.txt", "r", encoding="utf-8") as f:
            schools = [line.strip() for line in f if line.strip()]

        for school_edu in schools:
            tf = await trySchool(page, school_edu)
            if tf:
                with open("pa.txt", "a", encoding="utf-8") as out:
                    out.write(school_edu + "\n")

        await browser.close()

asyncio.run(go())