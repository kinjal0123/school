import asyncio
import re
import pandas as pd
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime

async def highlight_and_click(page, selector_or_locator, description="Action"):
    try:
        target = page.locator(selector_or_locator).first if isinstance(selector_or_locator, str) else selector_or_locator
        if await target.is_visible(timeout=2000):
            await target.evaluate("el => { el.style.border = '4px solid #00FF00'; el.style.backgroundColor = 'rgba(0,255,0,0.2)'; }")
            await target.click()
            return True
    except:
        pass
    return False

async def handle_cookies_automatically(page):
    cookie_selectors = ["text=Accept All", "text=Accept", "button:has-text('Accept')", "button:has-text('OK')", "#cookie-accept", ".cookie-button"]
    for selector in cookie_selectors:
        if await highlight_and_click(page, selector, "Cookie Banner"):
            break

def generate_fallback(metric, text_pool):
    metric_keywords = {
        "Coaching Credentials": ["teacher", "faculty", "staff", "qualified", "expert", "headmaster"],
        "Student Wellbeing": ["wellbeing", "pastoral", "support", "mental health", "care"],
        "Academic Integration": ["curriculum", "academic", "learning", "subjects", "education"],
        "Competitive Pathway": ["exam", "assessment", "result", "gcse", "a-level", "destination"],
        "Facilities & Resources": ["facilities", "campus", "sports", "library", "lab"],
        "Ongoing Accountability": ["progress", "tracking", "report", "feedback", "monitoring"]
    }
    kws = metric_keywords.get(metric, [])
    matched = [p for p in text_pool if any(k in p.lower() for k in kws)]
    if matched:
        return " ".join(matched[:2])
    return " ".join(text_pool[:2]) if text_pool else "Information not explicitly found on the site."

async def extract_school_data(url):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, slow_mo=300)
        context = await browser.new_context(viewport={'width': 1280, 'height': 800})
        page = await context.new_page()

        results = {
            "Name": "",
            "Founded": "", "City": "London, UK", "Ages": "", "Ratio": "N/A", "Fees": "Contact School",
            "About": "", "Philosophy": "",
            "Outcomes": "", "Admissions": "",
            "Performance": {},
            "URL": url, "Images": []
        }

        global_text_pool = []
        print(f"\nConnecting to: {url}...")

        try:
            await page.goto(url, wait_until="networkidle", timeout=60000)
            await handle_cookies_automatically(page)
            
            # Improved Name Extraction
            h1_text = await page.locator("h1").first.text_content() if await page.locator("h1").count() > 0 else ""
            title = await page.title()
            results["Name"] = h1_text.strip() if h1_text else title.split("|")[0].strip()

            body_text = await page.inner_text("body")

            def extract(pattern):
                match = re.search(pattern, body_text, re.IGNORECASE)
                return match.group(1).strip() if match else ""

            # IMPROVED FOUNDED LOGIC
            current_year = datetime.now().year
            clean_text = re.sub(r'\s+', ' ', body_text.lower())
            
            years = re.findall(r'\b(1[7-9]\d{2}|20\d{2})\b', clean_text)
            valid_years = [int(y) for y in years if 1700 <= int(y) <= current_year - 2]
            
            if valid_years:
                results["Founded"] = str(min(valid_years))

            # IMPROVED AGES LOGIC
            AGE_MAP = {
                "nursery": (3, 4), "reception": (4, 5), "pre-prep": (3, 7),
                "prep": (7, 11), "junior": (7, 11), "senior": (11, 18), "sixth form": (16, 18)
            }
            
            direct_age = extract(r'(?:Ages|Age Range)\s*[:\-]?\s*([\d\s–\-to]+)')
            if direct_age and len(direct_age) < 15:
                results["Ages"] = direct_age
            else:
                found_ages = [v for k, v in AGE_MAP.items() if k in clean_text]
                if found_ages:
                    results["Ages"] = f"{min(x[0] for x in found_ages)}–{max(x[1] for x in found_ages)}"

            # IMPROVED FEES LOGIC
            fee_match = re.search(r'£\s?(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)', body_text)
            if fee_match:
                results["Fees"] = f"£{fee_match.group(1)}"

            # IMAGE FETCHING
            image_set = set()
            img_elements = await page.query_selector_all('img')
            for img in img_elements:
                src = await img.get_attribute('src')
                if src:
                    full = urljoin(url, src)
                    if any(ext in full.lower() for ext in ['.jpg', '.jpeg', '.png']):
                        if 'logo' not in full.lower() and 'icon' not in full.lower():
                            image_set.add(full)
                if len(image_set) >= 10: break
            results["Images"] = list(image_set)

            # Link Discovery
            soup = BeautifulSoup(await page.content(), 'html.parser')
            links = soup.find_all('a', href=True)
            queue = []
            seen_urls = {url.rstrip('/')}
            targets = {
                "about": ["about", "history", "our-school", "welcome"],
                "outcomes": ["destination", "senior-school", "results", "beyond-prep"],
                "admission": ["admissions", "apply", "entry", "registrar", "fees"],
                "facilities": ["facilities", "campus", "co-curricular", "sports", "arts"]
            }

            for link in links:
                href, text = link['href'], link.get_text().lower().strip()
                full_url = urljoin(url, href).split('#')[0].rstrip('/')
                if full_url not in seen_urls and url in full_url:
                    if any(k in text or k in href.lower() for cat in targets.values() for k in cat):
                        queue.append((full_url, text))
                        seen_urls.add(full_url)

            # Deep Scraping
            for target_url, link_text in queue[:12]:
                try:
                    await page.goto(target_url, wait_until="domcontentloaded", timeout=30000)
                    await asyncio.sleep(1.5)
                    paragraphs = await page.locator("p").all_text_contents()
                    clean_paras = [p.strip() for p in paragraphs if len(p.strip()) > 60 and not any(x in p.lower() for x in ["cookie", "subscriber", "copyright"])]
                    
                    global_text_pool.extend(clean_paras)
                    page_content = " ".join(clean_paras)

                    if any(k in link_text or k in target_url for k in targets["about"]):
                        results["About"] = page_content[:1000]
                        results["Philosophy"] = " ".join([p for p in clean_paras if any(k in p.lower() for k in ["vision", "values", "ethos", "aims"])][:2])

                    if any(k in link_text or k in target_url for k in targets["outcomes"]):
                        results["Outcomes"] = page_content[:1000]

                    if any(k in link_text or k in target_url for k in targets["admission"]):
                        results["Admissions"] = " ".join([p for p in clean_paras if any(k in p.lower() for k in ["apply", "process", "register", "assessment"])][:4])

                except: continue

            # Final Metric Assignment
            perf_keywords = {
                "Coaching Credentials": ["teacher", "faculty", "staff"], 
                "Student Wellbeing": ["pastoral", "wellbeing", "care"],
                "Academic Integration": ["curriculum", "learning", "academic"], 
                "Competitive Pathway": ["exam", "results", "senior"],
                "Facilities & Resources": ["facilities", "campus", "grounds"], 
                "Ongoing Accountability": ["assessment", "progress", "tracking"]
            }
            for metric, kws in perf_keywords.items():
                match = [p for p in global_text_pool if any(k in p.lower() for k in kws)]
                results["Performance"][metric] = " ".join(match[:2]) if match else generate_fallback(metric, global_text_pool)

        except Exception as e:
            print(f"Error processing {url}: {e}")
        finally:
            await browser.close()

        # --- SAVE TO FILE LOGIC ---
        output = []
        output.append("\n" + "="*60)
        output.append(f"SCHOOL NAME: {results['Name']}")
        output.append(f"WEBSITE: {results['URL']}")
        output.append("="*60)
        output.append(f"Ages: {results['Ages']} | Founded: {results['Founded']} | City: {results['City']}")
        output.append(f"Annual Fees: {results['Fees']}")
        
        output.append(f"\n[ABOUT {results['Name'].upper()}]")
        output.append(results['About'] if results['About'] else "No specific 'About' section found.")

        output.append(f"\n[PHILOSOPHY & ETHOS]")
        output.append(results['Philosophy'] if results['Philosophy'] else "Information not found.")

        output.append(f"\n[OUTCOMES & DESTINATIONS]")
        output.append(results['Outcomes'] if results['Outcomes'] else "Information not found.")

        output.append(f"\n[ADMISSIONS PROCESS]")
        output.append(results['Admissions'] if results['Admissions'] else "Refer to website for details.")

        output.append(f"\n[PERFORMANCE METRICS]")
        for k, v in results["Performance"].items():
            output.append(f"- {k}: {v[:300]}...")

        output.append(f"\n[IMAGES FOUND: {len(results['Images'])}]")
        for i, img in enumerate(results["Images"], 1):
            output.append(f"{i}. {img}")
        
        output.append("\n" + "="*60 + "\n")

        full_text = "\n".join(output)
        
        # Print to console
        print(full_text)
        
        # Save to File (Append mode 'a')
        with open("output_results.txt", "a", encoding="utf-8") as f:
            f.write(full_text)

async def main():
    try:
        # Load Excel
        df = pd.read_excel("Site data .xlsx")
        
        # Filtering for London schools
        london_schools = df[df['address'].str.contains('London', case=False, na=False)]
        
        print(f"Found {len(london_schools)} schools in London. Starting extraction...")
        
        # Initialize/Clear the file for a new run
        with open("output_results.txt", "w", encoding="utf-8") as f:
            f.write(f"SCHOOL DATA EXTRACTION REPORT - {datetime.now()}\n")
            f.write("-" * 60 + "\n")

        for index, row in london_schools.iterrows():
            url = row['website']
            if pd.isna(url): continue
            if not str(url).startswith('http'): url = "https://" + str(url)
            
            await extract_school_data(url)
            
    except Exception as e:
        print(f"Error reading file or processing: {e}")

if __name__ == "__main__":
    asyncio.run(main())