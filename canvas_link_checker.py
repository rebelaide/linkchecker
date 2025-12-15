from __future__ import print_function
import re
import requests
import concurrent.futures
from bs4 import BeautifulSoup
from google.colab import userdata, auth
from google.auth import default
import gspread
from gspread_dataframe import set_with_dataframe
import pandas as pd
from urllib.parse import urlparse, urljoin
import cloudscraper
import time
import random

# --------------------------------------------------------------
# 1️⃣ CONSTANTS & CONFIGURATION
# --------------------------------------------------------------
try:
    CANVAS_API_URL = userdata.get('CANVAS_API_URL')
    CANVAS_API_KEY = userdata.get('CANVAS_API_KEY')
except Exception:
    # Fallback for manual entry if secrets aren't set
    CANVAS_API_URL = "https://your_canvas_domain.instructure.com"
    CANVAS_API_KEY = "your_api_key"

# Cloudscraper automatically handles User-Agents and Cloudflare challenges
scraper = cloudscraper.create_scraper(
    browser={
        'browser': 'chrome',
        'platform': 'windows',
        'desktop': True
    }
)

# ----------------------------------------------------------------------
# CanvasAPI Setup
# ----------------------------------------------------------------------
try:
    from canvasapi import Canvas
except ImportError as exc:
    raise ImportError("Please install canvasapi via `!pip install canvasapi`") from exc

# ----------------------------------------------------------------------
# Helper Functions
# ----------------------------------------------------------------------

def _get_domain(url):
    """Extracts domain from URL."""
    try:
        return urlparse(url).netloc
    except:
        return ""

def _extract_course_id(url):
    """Attempts to extract a course ID from a Canvas URL."""
    match = re.search(r'/courses/(\d+)', url)
    if match:
        return match.group(1)
    return None

def _is_valid_url(url):
    """Filters out mailto, javascript, and empty links."""
    return url and url.strip() and not url.startswith(('mailto:', 'javascript:', '#', 'tel:'))

def _check_link_status(args):
    """
    Checks the HTTP status of a single URL using Cloudscraper.
    Returns: (url, status_code, reason, is_redirect, final_url, is_canvas_link)
    """
    url, api_key = args
    
    # Random sleep to behave more like a human (prevent rate limiting)
    time.sleep(random.uniform(0.5, 1.5))

    is_canvas_link = _get_domain(CANVAS_API_URL) in url
    
    # Prepare headers specifically for internal Canvas links
    # For external links, we let Cloudscraper handle headers
    headers = {}
    if is_canvas_link:
        headers["Authorization"] = f"Bearer {api_key}"

    try:
        # Use scraper.get instead of requests.get
        # allow_redirects=True is default, but we check history manually if needed
        r = scraper.get(url, headers=headers, timeout=20, allow_redirects=True)
        
        status_code = r.status_code
        reason = r.reason
        is_redirect = len(r.history) > 0
        final_url = r.url
        
        return url, status_code, reason, is_redirect, final_url, is_canvas_link

    except requests.exceptions.ConnectionError:
        return url, 0, "Connection Error", False, "", is_canvas_link
    except requests.exceptions.Timeout:
        return url, 0, "Timeout", False, "", is_canvas_link
    except Exception as e:
        # Cloudscraper can sometimes raise specific Cloudflare errors
        return url, 0, f"Error: {str(e)}", False, "", is_canvas_link

def _extract_links_from_html(html, source_url, location_name):
    """Parses HTML and extracts a list of link dictionaries."""
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    found_links = []

    for a in soup.find_all("a"):
        href = a.get("href")
        text = a.get_text(strip=True)[:50]
        if _is_valid_url(href):
            full_url = urljoin(CANVAS_API_URL, href)
            found_links.append({
                "url": full_url,
                "text": text if text else "[Image/No Text]",
                "source_url": source_url,
                "location_name": location_name,
                "type": "Link"
            })

    for img in soup.find_all("img"):
        src = img.get("src")
        if _is_valid_url(src):
            full_url = urljoin(CANVAS_API_URL, src)
            found_links.append({
                "url": full_url,
                "text": f"Image: {img.get('alt', 'No Alt Text')}",
                "source_url": source_url,
                "location_name": location_name,
                "type": "Image"
            })
            
    for iframe in soup.find_all("iframe"):
        src = iframe.get("src")
        if _is_valid_url(src):
            full_url = urljoin(CANVAS_API_URL, src)
            found_links.append({
                "url": full_url,
                "text": "Iframe Embed",
                "source_url": source_url,
                "location_name": location_name,
                "type": "Iframe"
            })

    return found_links

# ----------------------------------------------------------------------
# MAIN FUNCTION
# ----------------------------------------------------------------------
def run_link_checker(course_input: str):
    """
    Scans a Canvas course for broken (4xx/5xx) or redirected links using Cloudscraper.
    """
    
    # 1. Authentication
    print("🔐 Authenticating with Google Sheets …")
    try:
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)
    except Exception as e:
        print(f"⚠️ Google Auth failed (Running locally?): {e}")
        gc = None
    
    canvas = Canvas(CANVAS_API_URL, CANVAS_API_KEY)
    
    if "courses/" in course_input:
        course_id = course_input.split("courses/")[-1].split("/")[0].split("?")[0]
    else:
        course_id = course_input.strip()

    try:
        course = canvas.get_course(course_id)
        print(f"\n📘 Processing Canvas Course: {course.name} ({course_id})\n")
    except Exception as e:
        print(f"❌ Error accessing course: {e}")
        return

    # 2. Scanning Content
    all_links = []
    
    print("🔎 Scanning Pages …")
    for p in course.get_pages():
        full_page = course.get_page(p.url)
        all_links.extend(_extract_links_from_html(full_page.body, p.html_url, f"Page: {p.title}"))

    print("🔎 Scanning Assignments …")
    for a in course.get_assignments():
        all_links.extend(_extract_links_from_html(a.description, a.html_url, f"Assignment: {a.name}"))

    print("🔎 Scanning Discussions …")
    for d in course.get_discussion_topics():
        all_links.extend(_extract_links_from_html(d.message, d.html_url, f"Discussion: {d.title}"))

    print("🔎 Scanning Syllabus …")
    try:
        course_with_syll = canvas.get_course(course_id, include="syllabus_body")
        syllabus_body = getattr(course_with_syll, "syllabus_body", "")
        if syllabus_body:
             all_links.extend(_extract_links_from_html(syllabus_body, f"{CANVAS_API_URL}/courses/{course_id}/assignments/syllabus", "Syllabus"))
    except Exception:
        print("⚠️ Could not check Syllabus.")

    print("🔎 Scanning Announcements …")
    for ann in course.get_discussion_topics(only_announcements=True):
        all_links.extend(_extract_links_from_html(ann.message, ann.html_url, f"Announcement: {ann.title}"))

    print("🔎 Scanning Modules (External URL Items) …")
    for mod in course.get_modules():
        for item in mod.get_module_items():
            if item.type == 'ExternalUrl':
                all_links.append({
                    "url": item.external_url,
                    "text": "Module External URL",
                    "source_url": f"{CANVAS_API_URL}/courses/{course_id}/modules",
                    "location_name": f"Module: {mod.name} / Item: {item.title}",
                    "type": "Module Item"
                })

    # 3. Deduplicate URLs
    unique_urls = list(set([item['url'] for item in all_links]))
    print(f"\n🔗 Found {len(all_links)} total links. Checking {len(unique_urls)} unique URLs ...")

    # 4. Check Links (Reduced Threads for Stealth)
    url_results = {}
    
    # Reduced max_workers to 5 to avoid triggering aggressive firewalls
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        tasks = [(url, CANVAS_API_KEY) for url in unique_urls]
        results = list(executor.map(_check_link_status, tasks))
        
        for url, status, reason, is_redirect, final_url, is_canvas_link in results:
            url_results[url] = {
                "status": status,
                "reason": reason,
                "redirect": is_redirect,
                "final_url": final_url,
                "is_canvas": is_canvas_link
            }

    # 5. Compile Report Data
    report_rows = []
    
    for link in all_links:
        res = url_results.get(link['url'])
        if not res:
            continue
            
        status_code = res['status']
        is_redirect = res['redirect']
        is_canvas = res['is_canvas']
        
        issue_type = None
        
        # Issue Classification Logic
        if status_code >= 500:
            issue_type = "Server Error (5xx)"
        elif is_canvas and status_code in [401, 403]:
            link_course_id = _extract_course_id(link['url'])
            if link_course_id and str(link_course_id) != str(course_id):
                issue_type = "Inaccessible Canvas Course (Other Course)"
            else:
                issue_type = "Access Denied (Locked Content)"
        elif status_code >= 400:
            issue_type = "Broken Link (4xx)"
        elif status_code == 0:
            issue_type = "Connection Failed"
        elif is_redirect:
            issue_type = "Redirect"
        
        if issue_type:
            row = {
                "Issue Type": issue_type,
                "Location": link['location_name'],
                "Status Code": status_code,
                "Status Msg": res['reason'],
                "Link Text": link['text'],
                "Original URL": link['url'],
                "Final URL": res['final_url'] if is_redirect else "",
                "Canvas Edit Link": link['source_url']
            }
            report_rows.append(row)

    df = pd.DataFrame(report_rows)

    # 6. Export Report
    print(f"\n📊 Processing {len(report_rows)} issues found …")
    
    if df.empty:
        print("✅ No broken, redirected, or inaccessible links found!")
        return

    df.sort_values(by=["Issue Type", "Location"], inplace=True)
    
    # Handle CSV Fallback if Google Sheets is not available (e.g. local run)
    if gc is None:
        csv_name = f"Link_Report_{course_id}.csv"
        df.to_csv(csv_name, index=False)
        print(f"💾 Google Auth unavailable. Saved as CSV: {csv_name}")
        return

    sheet_title = f"{course.name} Link Report"
    
    try:
        existing_sheets = gc.list_spreadsheet_files()
        sheet = next((s for s in existing_sheets if s["name"] == sheet_title), None)
        
        if sheet:
            print(f"♻️  Found existing sheet: {sheet_title}. Replacing contents …")
            sh = gc.open_by_key(sheet["id"])
            ws = sh.sheet1
            ws.clear()
        else:
            print(f"🆕 Creating new sheet: {sheet_title}")
            sh = gc.create(sheet_title)
            ws = sh.sheet1
            
        set_with_dataframe(ws, df)
        ws.format('A1:H1', {'textFormat': {'bold': True}})
        ws.freeze(rows=1)

        print(f"\n✅ Report complete!")
        print(f"📎 Google Sheet URL: {sh.url}")
        
    except Exception as e:
        print(f"❌ Error writing to Google Sheet: {e}")
        df.to_csv("link_report.csv", index=False)
        print(f"💾 Saved as CSV instead.")

if __name__ == "__main__":
    # Allow running directly from command line if desired
    # Example: python canvas_link_checker.py
    import sys
    if len(sys.argv) > 1:
        run_link_checker(sys.argv[1])
