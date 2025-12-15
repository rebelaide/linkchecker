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
import time
from urllib.parse import urlparse, urljoin

# --------------------------------------------------------------
# 1️⃣ CONSTANTS & CONFIGURATION
# --------------------------------------------------------------
try:
    CANVAS_API_URL = userdata.get('CANVAS_API_URL')
    CANVAS_API_KEY = userdata.get('CANVAS_API_KEY')
except Exception:
    # Fallback if secrets aren't set (for local testing, though Colab secrets are preferred)
    CANVAS_API_URL = "https://your_canvas_domain.instructure.com"
    CANVAS_API_KEY = "your_api_key"

# User-Agent to prevent 403s from strict servers (like Wikipedia/Amazon)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# ----------------------------------------------------------------------
# CanvasAPI Setup
# ----------------------------------------------------------------------
try:
    from canvasapi import Canvas
except ImportError as exc:
    raise ImportError("Please install canvasapi via `!pip install canvasapi`") from exc

# ----------------------------------------------------------------------
# Helper Functions: Link Extraction & Checking
# ----------------------------------------------------------------------

def _get_domain(url):
    """Extracts domain from URL for internal link checking."""
    try:
        return urlparse(url).netloc
    except:
        return ""

def _is_valid_url(url):
    """Filters out mailto, javascript, and empty links."""
    return url and url.strip() and not url.startswith(('mailto:', 'javascript:', '#', 'tel:'))

def _check_link_status(args):
    """
    Checks the HTTP status of a single URL.
    Returns: (url, status_code, reason, is_redirect, final_url)
    """
    url, api_key = args
    
    # Prepare headers
    req_headers = HEADERS.copy()
    
    # If it's an internal Canvas link, add the Authorization header
    if _get_domain(CANVAS_API_URL) in url:
        req_headers["Authorization"] = f"Bearer {api_key}"

    try:
        # We use stream=True to avoid downloading large files just to check headers
        # We allow redirects to track them, but we check history to see if it happened
        r = requests.get(url, headers=req_headers, timeout=10, stream=True, verify=False) # verify=False helps with some weird SSL certs, use with caution
        
        status_code = r.status_code
        reason = r.reason
        is_redirect = len(r.history) > 0
        final_url = r.url
        
        # Close connection explicitly
        r.close()
        
        return url, status_code, reason, is_redirect, final_url

    except requests.exceptions.ConnectionError:
        return url, 0, "Connection Error", False, ""
    except requests.exceptions.Timeout:
        return url, 0, "Timeout", False, ""
    except requests.exceptions.RequestException as e:
        return url, 0, f"Error: {str(e)}", False, ""

def _extract_links_from_html(html, source_url, location_name):
    """Parses HTML and extracts a list of link dictionaries."""
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    found_links = []

    # 1. Check Anchor tags (href)
    for a in soup.find_all("a"):
        href = a.get("href")
        text = a.get_text(strip=True)[:50] # First 50 chars of link text
        if _is_valid_url(href):
            # Resolve relative URLs
            full_url = urljoin(CANVAS_API_URL, href)
            found_links.append({
                "url": full_url,
                "text": text if text else "[Image/No Text]",
                "source_url": source_url,
                "location_name": location_name,
                "type": "Link"
            })

    # 2. Check Images (src)
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
            
    # 3. Check Iframes (src)
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
    Scans a Canvas course for broken/redirected links and saves to Google Sheets.
    """
    
    # 1. Authentication
    print("🔐 Authenticating with Google Sheets …")
    auth.authenticate_user()
    creds, _ = default()
    gc = gspread.authorize(creds)
    
    canvas = Canvas(CANVAS_API_URL, CANVAS_API_KEY)
    
    # Resolve Course ID
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
    
    # --- Pages ---
    print("🔎 Scanning Pages …")
    for p in course.get_pages():
        # Fetch full body (list endpoint doesn't return body)
        full_page = course.get_page(p.url)
        all_links.extend(_extract_links_from_html(full_page.body, p.html_url, f"Page: {p.title}"))

    # --- Assignments ---
    print("🔎 Scanning Assignments …")
    for a in course.get_assignments():
        all_links.extend(_extract_links_from_html(a.description, a.html_url, f"Assignment: {a.name}"))

    # --- Discussions ---
    print("🔎 Scanning Discussions …")
    for d in course.get_discussion_topics():
        all_links.extend(_extract_links_from_html(d.message, d.html_url, f"Discussion: {d.title}"))

    # --- Syllabus ---
    print("🔎 Scanning Syllabus …")
    try:
        settings = course.get_settings() # Sometimes syllabus is here
        # Easier method: reload course with include parameter
        course_with_syll = canvas.get_course(course_id, include="syllabus_body")
        syllabus_body = getattr(course_with_syll, "syllabus_body", "")
        if syllabus_body:
             all_links.extend(_extract_links_from_html(syllabus_body, f"{CANVAS_API_URL}/courses/{course_id}/assignments/syllabus", "Syllabus"))
    except Exception:
        print("⚠️ Could not check Syllabus.")

    # --- Announcements ---
    print("🔎 Scanning Announcements …")
    for ann in course.get_discussion_topics(only_announcements=True):
        all_links.extend(_extract_links_from_html(ann.message, ann.html_url, f"Announcement: {ann.title}"))

    # --- Modules (External URLs) ---
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

    # 3. Deduplicate URLs for efficiency
    # We want to check each unique URL once, then map results back
    unique_urls = list(set([item['url'] for item in all_links]))
    print(f"\n🔗 Found {len(all_links)} total links. Checking {len(unique_urls)} unique URLs ...")

    # 4. Check Links (Parallel)
    # Using a dictionary to store results: {url: (status, reason, is_redirect, final_url)}
    url_results = {}
    
    # Suppress SSL warnings for cleaner output
    requests.packages.urllib3.disable_warnings()

    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        # Create tasks tuple (url, api_key)
        tasks = [(url, CANVAS_API_KEY) for url in unique_urls]
        
        # Map tasks
        results = list(executor.map(_check_link_status, tasks))
        
        for url, status, reason, is_redirect, final_url in results:
            url_results[url] = {
                "status": status,
                "reason": reason,
                "redirect": is_redirect,
                "final_url": final_url
            }

    # 5. Compile Report Data
    report_rows = []
    
    for link in all_links:
        res = url_results.get(link['url'])
        if not res:
            continue
            
        status_code = res['status']
        is_redirect = res['redirect']
        
        # FILTER: Only report Broken (400+), Redirects (300+ or history), or Errors (0)
        # We generally treat 200 as success and exclude it, unless it was a redirect
        is_broken = status_code >= 400 or status_code == 0
        
        if is_broken or is_redirect:
            row = {
                "Location": link['location_name'],
                "Link Text/Alt": link['text'],
                "Original URL": link['url'],
                "Status Code": status_code,
                "Status": res['reason'],
                "Issue Type": "Broken Link" if is_broken else "Redirect",
                "Final URL (if redirect)": res['final_url'] if is_redirect else "",
                "Canvas Link": link['source_url']
            }
            report_rows.append(row)

    df = pd.DataFrame(report_rows)

    # 6. Export to Google Sheets
    print(f"\n📊 Processing {len(report_rows)} issues found …")
    
    if df.empty:
        print("✅ No broken or redirected links found!")
        return

    # Sort: Broken links first, then redirects
    df.sort_values(by=["Issue Type", "Location"], inplace=True)

    sheet_title = f"{course.name} Link Report"
    
    try:
        # Check if sheet exists
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
        
        # Apply basic formatting (frozen header)
        ws.format('A1:H1', {'textFormat': {'bold': True}})
        ws.freeze(rows=1)

        print(f"\n✅ Report complete!")
        print(f"📎 Google Sheet URL: {sh.url}")
        
    except Exception as e:
        print(f"❌ Error writing to Google Sheet: {e}")
        # Fallback to CSV if Sheets fails
        csv_name = "link_report.csv"
        df.to_csv(csv_name, index=False)
        print(f"💾 Saved as CSV instead: {csv_name}")

# Execute
# Replace with your course ID or URL below when calling the function
# run_link_checker("123456")
