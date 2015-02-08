def sanitize(word):
    """Strip special characters."""
    # !!! TODO: add handler for aphostrophes
    # strip from the back
    while len(word) > 0 and not word[-1].isalnum():
        word = word[:-1]

    # strip from the front
    while len(word) > 0 and not word[0].isalnum():
        word = word[1:]

    return word


def Map(word):
    # ignore case distinctions
    word = word.lower()

    # check for special characters
    if not word.isalnum():
        word = sanitize(word)

    return word, 1


def partition(lst):
    grouped_items = {}
    for tup in lst:
        try:
            grouped_items[tup[0]].append(tup)
        except KeyError:
            grouped_items[tup[0]] = [tup]

    return grouped_items


def Reduce(mapping):
    return (mapping[0], sum(x[1] for x in mapping[1]))
