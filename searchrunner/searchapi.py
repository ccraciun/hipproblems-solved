import heapq
import json

from tornado import gen, ioloop, web
from tornado import httpclient


class SearchApiHandler(web.RequestHandler):
    SCRAPER_URL_FMT = "http://localhost:9000/scrapers/{}"
    PROVIDERS = ("Expedia", "Orbitz", "Priceline", "Travelocity", "United")

    @gen.coroutine
    def get(self):
        client = httpclient.AsyncHTTPClient()

        responses = yield {p: client.fetch(self.SCRAPER_URL_FMT.format(p)) for p in self.PROVIDERS}

        results = {}
        for provider, response in responses.items():
            if response.error:
                continue
            try:
                scrape = json.loads(response.body)
                results[provider] = ((res['agony'], res) for res in scrape['results'])
            except:
                raise

        merged = heapq.merge(*results.values())
        results = (res[1] for res in merged)

        self.write({
            "results": list(results),
        })


ROUTES = [
    (r"/flights/search", SearchApiHandler),
]


def run():
    app = web.Application(
        ROUTES,
        debug=True,
    )

    app.listen(8000)
    print "Server (re)started. Listening on port 8000"

    ioloop.IOLoop.current().start()


if __name__ == "__main__":
    run()
