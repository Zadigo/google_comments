
# Script used to get the informations for a
# given business on a Google Place url:
# e.g. Aprium Grande Pharmacie De Paris,
# https://www.google.com/maps/place/Aprium+Grande+Pharmacie+De+Paris/@50.6368743,3.0663956,17z/data=!3m1!4b1!4m6!3m5!1s0x47c2d58a29e20067:0xcac399b7db71114c!8m2!3d50.6368743!4d3.0689705!16s%2Fg%2F11b69zq7kb?entry=ttu
# will return specific pieces of information for
# this particular business

BUSINESS_INFORMATION_SCRIPT = """
    function getText (el) {
        return el && el.textContent.trim()
    }

    function resolveXpath (xpath) {
        return document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue
    }

    function evaluateXpath (xpath) {
        var result = resolveXpath(xpath)
        return getText(result)
    }

    function getBusiness () {
        let name = (
            document.querySelector('div[role="main"]').ariaLabel ||
            // As fallback, get all [role="main"] and only select the one with an
            // AriaLabel to get business name
            Array.from(document.querySelectorAll('div[role="main"][aria-label]'))[0].ariaLabel
        )
        let address = (
            evaluateXpath('//button[contains(@aria-label, "Adresse:")]') ||
            evaluateXpath('//button[contains(@aria-label, "Address:")]')
        )
        let business_rating = document.querySelector('span[role="img"]').ariaLabel
        let numberOfReviews = evaluateXpath('//div[contains(@class, "F7nice")]/span[2]')
        let telephone = (
            evaluateXpath('//button[contains(@data-tooltip, "Copier le numéro de téléphone")][contains(@aria-label, "téléphone:")]') ||
            evaluateXpath('//button[contains(@aria-label, "Phone:")]')
        )
        let category = evaluateXpath('//button[contains(@jsaction, "pane.rating.category")]')

        let websiteElement = (
            resolveXpath('//a[contains(@aria-label, "Site Web:")]') ||
            resolveXpath('//a[contains(@aria-label, "Website:")]')
        )
        let website = websiteElement && websiteElement.href

        let businessType = getText(resolveXpath('//button[contains(@jsaction, "\.category")]'))
        let permanentlyClosed = resolveXpath('//*[contains(text(), "Définitivement fermé")]')

        return {
            name,
            url: window.location.href,
            address,
            business_rating,
            number_of_reviews: numberOfReviews,
            telephone,
            website,
            business_type: businessType,
            permanently_closed: permanentlyClosed !== null,
            additional_information: (
                evaluateXpath('//div[contains(@aria-label, "Informations")][@role="region"][contains(@class, "m6QErb")]') ||
                evaluateXpath('//div[contains(@aria-label, "Information")][@role="region"][contains(@class, "m6QErb")]')
            )
        }
    }

    return getBusiness()
"""


# This script will collect details from reviews
# such as text, rating, reviewer name etc.
# on a Google Place url. This script will work
# both on the Google Place "Presentation" tab
# on the the "Reviews" tab. See url above.

COMMENTS_SCRIPT = """
    function getText (el) {
        return el && el.textContent.trim()
    }

    function resolveXpath (xpath) {
        return document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue
    }

    function evaluateXpath (xpath) {
        var result = resolveXpath(xpath)
        return getText(result)
    }

    function gatherComments() {
        const commentsWrapper = document.querySelectorAll("div[data-review-id^='Ch'][class*='fontBodyMedium ']")

        Array.from(commentsWrapper).forEach((item) => {
            let dataReviewId = item.dataset['reviewId']
            try {
                // Sometimes there is a read more button
                // that we have to click

                moreButton = (
                    // Try the "Voir plus" button"
                    item.querySelector('button[aria-label="Voir plus"]') ||
                    // Try the "See more" button"
                    item.querySelector('button[aria-label="See more"]') ||
                    // On last resort try "aria-expanded"
                    item.querySelector('button[aria-expanded="false"]')
                )
                moreButton.click()
            } catch (e) {
                console.log('No "see more" button for review', dataReviewId)
            }
        })

        return Array.from(commentsWrapper).map((item) => {
            let dataReviewId = item.dataset['reviewId']

            // Or, .rsqaWe
            let period = getText(item.querySelector('.DU9Pgb'))
            let rating = item.querySelector('span[role="img"]') && item.querySelector('span[role="img"]').ariaLabel
            let text = getText(item.querySelector("*[class='MyEned']"))
            let reviewerName = getText(item.querySelector('[class*="d4r55"]'))
            let reviewerNumberOfReviews = getText(item.querySelector('*[class*="RfnDt"]'))

            return {
                google_review_id: dataReviewId,
                text,
                rating,
                period,
                reviewer_name: reviewerName,
                reviewer_number_of_reviews: reviewerNumberOfReviews
            }
        })
    }

    return gatherComments()
"""


# Tests if the currentpage is a feed page 
# or a Google Place page
IS_FEED_PAGE_SCRIPT = """
    function resolveXpath (xpath) {
        return document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue
    }
    const result = resolveXpath('//div[contains(@aria-label, "Résultats pour")][@role="feed"]')
    return result !== null
"""


# Use this script to return information such
# as address, telephone... for a given business.
# This script works when a Google Place card
# has been clicked to display the modal to get 
# additional information on the 
# business (or "Overview" tab)
ADDRESS_SCRIPT = """
    function getText (el) {
        return el && el.textContent
    }

    function resolveXpath (xpath) {
        return document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue
    }

    function evaluateXpath (xpath) {
        var result = resolveXpath(xpath)
        return getText(result)
    }

    return {
        feed_url: window.location.href,
        address: evaluateXpath('//button[contains(@data-item-id, "address")]'),
        telephone: evaluateXpath('//button[contains(@data-item-id, "tel")]'),
        website: evaluateXpath('//a[contains(@data-item-id, "authority")]'),
        raw_information: evaluateXpath('//div[contains(@aria-label, "Informations")][@role="region"][contains(@class, "m6QErb")]')
    }
"""


GOOGLE_REVIEWS_FROM_GOOGLE_SEARCH = """
function getText (el) {
    return el && el.textContent
}

function clickElement (el) {
    el && el.click()
}

function parseComments () {
    return Array.from(document.querySelectorAll('.gws-localreviews__google-review')).map(x => {
        const readMore = x.querySelector('a[class="review-more-link"][role="button"][aria-expanded="false"]')
        clickElement(readMore)

        const ratingElement = x.querySelector('span[aria-label][role="img"]')
        const periodElement = ratingElement.nextElementSibling
        const info = getText(x.querySelector('.A503be'))
        return {
            google_review_id: null,
            snippet: getText(x.querySelector('span[class="review-snippet"]')),
            text: (
                getText(x.querySelector('span[class="review-full-text"]')) ||
                getText(x.querySelector('.Jtu6Td'))
            ),
            rating: ratingElement.ariaLabel,
            period: getText(periodElement),
            reviewer_name: x.querySelector('img').alt,
            reviewer_number_of_reviews: info
        }
    })
}

function loadComments() {
    setTimeout(() => {
        document.querySelector('.YFJBee').scrollIntoView({ behavior: "smooth", block: "center", inline: "nearest" })
        document.querySelector('.YFJBee').click()
    }, 2000)
    const comments = parseComments()
    localStorage.setItem('coments', JSON.stringify(comments))
    return comments
}

return loadComments()
"""
