from bs4 import BeautifulSoup
import re
import html


def clean_html(html_content: str) -> str:
    """Removes HTML tags, decodes entities, and cleans text."""
    soup = BeautifulSoup(html_content, "html.parser")
    text = html.unescape(soup.get_text(separator=" ")).strip()
    return re.sub(r"\s+", " ", text)


if __name__ == "__main__":
    # Example usage
    sample_html = "<p><strong>Welcome!</strong> This is <a href='#'>an example</a>. &copy; 2024</p>"
    cleaned_text = clean_html(sample_html)
    print(
        f"Cleaned Text: {cleaned_text}"
    )  # Output: "Welcome! This is an example. © 2024"
