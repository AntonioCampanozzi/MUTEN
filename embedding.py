from sentence_transformers import SentenceTransformer
import numpy as np
from timestamp_embedder import TimeEmbedder


sbert_model = SentenceTransformer('sentence-transformers/all-roberta-large-v1')
from sklearn.feature_extraction.text import CountVectorizer

def get_feature_ranges(embeddings):
    """
    Calcola i range delle features degli embeddings.

    :param embeddings: matrice degli embeddings
    :return: min, max e range delle features
    """
    feature_min = np.min(embeddings, axis=0)
    feature_max = np.max(embeddings, axis=0)
    feature_range = feature_max - feature_min
    return feature_min, feature_max, feature_range


def get_variants_embeddings_agg(variants):
    """
    Calcola gli embeddings delle varianti utilizzando il modello SBERT.

    :param variants: lista di varianti
    :return: matrice degli embeddings
    """
    new_variants = []
    #print(variants)
    for v in variants:
        v_new = v.split(' -> ')
        l = " ".join([l.replace(' ', '') for l in v_new])
        new_variants.append(l)
    vectorizer = CountVectorizer()
    X = vectorizer.fit_transform(new_variants)
    return X.toarray()#sbert_model.encode(variants)

def get_sentence_embeddings(variants):
    """
    Calcola gli embeddings delle varianti utilizzando il modello SBERT.

    :param variants: lista di varianti
    :return: matrice degli embeddings
    """
    return sbert_model.encode(variants)

def log_scale_time_deltas(time_deltas):
    """
    Applica una scala logaritmica ai time deltas.

    :param time_deltas: array dei time deltas
    :return: array dei time deltas scalati
    """
    return np.log1p(time_deltas)

def get_time_embeddings(autoencoder, time_deltas):
    """
    Calcola gli embeddings dei time deltas scalati logaritmicamente.

    :param time_deltas: array dei time deltas
    :return: matrice degli embeddings
    """
    encoder = Model(
        inputs=autoencoder.model.input,
        outputs=autoencoder.model.layers[3].output  # Dense(128)
        )
    
    return encoder.predict(time_deltas)