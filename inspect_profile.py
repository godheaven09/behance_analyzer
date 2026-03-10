"""Inspect real Behance profile HTML to find correct selectors for PRO, Services, Banner."""
import asyncio
from playwright.async_api import async_playwright

PROFILES = [
    "https://www.behance.net/superflash",       # Тимофей
    "https://www.behance.net/neinna",           # Inna (unlikely PRO)
    "https://www.behance.net/1e43f9e9",         # Ксения (3 followers)
]

async def inspect(url):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36")
        
        print(f"\n{'='*60}")
        print(f"  {url}")
        print(f"{'='*60}")
        
        await page.goto(url, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(2)

        # Search for PRO badge by looking at text content
        all_els = await page.evaluate("""() => {
            const results = [];
            const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_ELEMENT);
            let count = 0;
            while (walker.nextNode() && count < 2000) {
                const el = walker.currentNode;
                const text = el.textContent?.trim() || '';
                const cls = el.className || '';
                const tag = el.tagName;
                
                // Look for PRO indicators
                if ((typeof cls === 'string' && (cls.toLowerCase().includes('pro') || cls.toLowerCase().includes('badge'))) 
                    || (text === 'PRO' || text === 'Pro')) {
                    if (text.length < 100) {
                        results.push({tag, cls: String(cls).substring(0, 80), text: text.substring(0, 50), type: 'pro'});
                    }
                }
                
                // Look for Services
                if (typeof cls === 'string' && cls.toLowerCase().includes('service')) {
                    if (text.length < 200) {
                        results.push({tag, cls: String(cls).substring(0, 80), text: text.substring(0, 50), type: 'service'});
                    }
                }
                
                count++;
            }
            return results;
        }""")
        
        print("\n--- PRO/Badge/Service elements ---")
        for el in all_els:
            print(f"  [{el['type']}] <{el['tag']}> class=\"{el['cls']}\" => \"{el['text']}\"")

        # Also check page source for "ProBadge" or "pro" patterns
        html = await page.content()
        import re
        pro_classes = re.findall(r'class="[^"]*[Pp]ro[^"]*"', html)
        unique_pro = set(pro_classes)
        print(f"\n--- Unique class attrs containing 'pro' ({len(unique_pro)}) ---")
        for c in sorted(unique_pro)[:20]:
            print(f"  {c[:100]}")

        # Check for specific PRO badge SVG or text
        pro_badge = await page.query_selector('text=PRO')
        if pro_badge:
            parent_html = await pro_badge.evaluate("el => el.parentElement?.outerHTML?.substring(0, 300)")
            print(f"\n--- Exact 'PRO' text element parent ---")
            print(f"  {parent_html}")
        else:
            print("\n--- No exact 'PRO' text found ---")

        # Check for services tab/section  
        svc_tab = await page.query_selector('text=Services')
        svc_tab_ru = await page.query_selector('text=Услуги')
        print(f"\n--- Services tab: EN={svc_tab is not None}, RU={svc_tab_ru is not None} ---")

        await browser.close()


async def main():
    for url in PROFILES:
        try:
            await inspect(url)
        except Exception as e:
            print(f"Error for {url}: {e}")

asyncio.run(main())
