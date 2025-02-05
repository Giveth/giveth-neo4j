from bs4 import BeautifulSoup
import re
import html

def clean_html(html_content):
    """Removes HTML tags, decodes entities, and cleans text."""
    # 1️⃣ Parse the HTML
    soup = BeautifulSoup(html_content, "html.parser")
    
    # 2️⃣ Extract only text
    text = soup.get_text(separator=" ")

    # 3️⃣ Decode HTML entities (e.g., &amp; → &)
    text = html.unescape(text)

    # 4️⃣ Remove excessive spaces & newlines
    text = re.sub(r"\s+", " ", text).strip()

    return text

# Test Example
if __name__ == "__main__":
    sample_html = "<p><strong>Welcome!</strong> This is <a href='#'>an example</a>. &copy; 2024</p>"
    cleaned_text = clean_html(sample_html)
    print(cleaned_text)  # Output: "Welcome! This is an example. © 2024"
