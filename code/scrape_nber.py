import requests
from bs4 import BeautifulSoup 
import re

"""Takes the RA listings on the NBER website that are not at the NBER and returns job title, NBER-sponsored researcher, institution, research field, and link to job posting"""

response = requests.get("https://www.nber.org/career-resources/research-assistant-positions-not-nber")
html = response.text
soup = BeautifulSoup(html, "html.parser")
text = soup.find_all("div", {"class": "page-header__intro-inner"})[0]
posts = text.find_all("p")
   
clean_posts = posts[2:-2]

final_posts = re.sub("(<!--.*?-->)", "", str(clean_posts))

# output = []
# for n in range(len(clean_posts)):
#     title = clean_posts[n].contents[0] if clean_posts[n].contents[0] else "titlemissing"
#     researcher = clean_posts[n].contents[2] if clean_posts[n].contents[2] else "researchermissing"
#     institution = clean_posts[n].contents[5] if clean_posts[n].contents[5] else "instmissing"
#     research_field = clean_posts[n].contents[7] if clean_posts[n].contents[7] else "fieldmissing"
#     link = clean_posts[n].a["href"] if clean_posts[n].a["href"] else "linkmissing"
#     output.append([title, researcher, institution, research_field, link])