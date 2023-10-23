from goose3 import Goose


def scrape_from_url(news_dict, skip_on_error=False):
    url = news_dict["url"]
    g = Goose()
    print("Scraping URL: ", url)

    if skip_on_error:
        try:
            article = g.extract(url=url)
            news_dict["full_text"] = article.cleaned_text
        except:
            print("Error scraping URL: ", url)
            news_dict["full_text"] = article.cleaned_text
    else:
        article = g.extract(url=url)
        news_dict["full_text"] = article.cleaned_text

    return news_dict
