from sentence_transformers import SentenceTransformer
import numpy as np
from scipy.fftpack import dct
from sklearn.decomposition import PCA

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
    pca= PCA(n_components=5)
    
    return pca.fit_transform(sbert_model.encode(variants))

def log_scale_time_deltas(time_deltas):
    """
    Applica una scala logaritmica ai time deltas.

    :param time_deltas: array dei time deltas
    :return: array dei time deltas scalati
    """
    return np.log1p(time_deltas)

def get_time_embeddings(sequences, percentile=95):
    """
    Calcola gli embeddings temporali utilizzando il Deterministic Cosine Transform.

    :param sequences: lista di sequenze temporali
    :param percentile: percentile per la selezione della dimensione dell'embedding
    :return: matrice degli embeddings temporali
    """
    lengths = np.array([len(seq) for seq in sequences])
    embedding_dim = int(np.percentile(lengths, percentile))
    print(embedding_dim)
    embeddings = []
    pca= PCA(n_components=4)
    for s in sequences:
        s = np.array(s)
        dct_coefficients = dct(s, type=2, axis=0, norm='ortho')
        if len(dct_coefficients) < embedding_dim:
            dct_coefficients = np.pad(dct_coefficients, (0, embedding_dim - len(dct_coefficients)), constant_values=0.0)
        else:
            dct_coefficients = dct_coefficients[:embedding_dim]
        print(dct_coefficients.shape)
        embeddings.append(dct_coefficients)
    return pca.fit_transform(np.vstack(embeddings))

def concat_embeddings(emb1, emb2):
    """
    Normalizza e concatena le due tipologie di embeddings.

    :param emb1: prima matrice di embeddings
    :param emb2: seconda matrice di embeddings
    :return: matrice di embeddings concatenata
    """
    
    #emb1=emb1/np.linalg.norm(emb1, axis=1, keepdims=True)
    
    print(type(emb1), emb1.shape, emb1.dtype)
    print(type(emb2), emb2.shape, emb2.dtype)

    return np.concatenate((emb1, emb2), axis=1)