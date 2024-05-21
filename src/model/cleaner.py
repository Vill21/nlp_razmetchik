import re
import functools
import nltk
from natasha import (
    Doc,
    Segmenter,
    MorphVocab,
    NewsEmbedding,
    NewsMorphTagger
)


def composite(*functions):
    def compose(f, g):
        return lambda x: f(g(x))
    return functools.reduce(compose, functions, lambda x: x)


def remove_english(text: str) -> str:
    return re.sub(r'[a-zA-Z]+', ' ', text)


def remove_punctuation(text: str) -> str:
    return re.sub(r'[!"#$%&\'()*+,\-./:;<=>?@\[\\\]^_`{|}~]', ' ', text)


def remove_whitespace(text: str) -> str:
    return re.sub(r'\s+', ' ', text)


def remove_stopwords(text: str) -> str:
    sw = nltk.corpus.stopwords.words('russian')
    return " ".join([word.lower() for word in text.split() if word.lower() not in sw])


def lemmatize(text: str) -> str:
    doc: Doc = Doc(text)
    doc.segment(Segmenter())
    morphVocab: MorphVocab = MorphVocab()

    emb: NewsEmbedding = NewsEmbedding()
    morph_tagger: NewsMorphTagger = NewsMorphTagger(emb)
    doc.tag_morph(morph_tagger)

    for token in doc.tokens:
        token.lemmatize(morphVocab)

    return " ".join([token.lemma for token in doc.tokens])


def remove_duplicates(text: str) -> str:
    return " ".join(list(set(text.split(" "))))


def clean(text: str) -> str:
    return composite(
        remove_duplicates,
        lemmatize,
        remove_stopwords,
        remove_whitespace,
        remove_punctuation,
        remove_english
    )(text.lower())


# if __name__ == '__main__':
#     import pandas as pd
#
#     nltk.download('stopwords')
#
#     with open("./data/toxic_pseudolegal.txt", "r") as file:
#         df = pd.DataFrame({"text": file.read().splitlines()})
#
#     df["text"] = df["text"].apply(lambda x: clean(x))
#     print(df.head(10))
