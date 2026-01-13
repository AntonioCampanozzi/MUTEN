from sklearn.cluster import KMeans
from sklearn_extra.cluster import KMedoids
from sklearn.metrics import pairwise_distances
from kneed import KneeLocator
import numpy as np

def composite_distance_matrix(X_sem, X_tmp):
    """
    Calcola una matrice di distanza composita combinando distanze semantiche e temporali.
    :param X_sem: matrice degli embeddings semantici
    :param X_tmp: matrice degli embeddings temporali
    :return: matrice di distanza composita
    """
    
    d_sem = pairwise_distances(X_sem, metric="cosine")
    d_tmp = pairwise_distances(X_tmp, metric="cosine")

    # normalizzazione 
    d_sem /= d_sem.max()
    d_tmp /= d_tmp.max()

    return d_sem + d_tmp


def run_kmeans_elbow(distance_matrix, k_min=2, k_max=15, random_state=42):
    """
    Esegue il metodo dell'elbow per determinare il numero ottimale di cluster e applica il kmeans ottimale.

    :param embeddings: matrice degli embeddings
    :param k_min: numero minimo di cluster
    :param k_max: numero massimo di cluster
    :param random_state: seed per la riproducibilità
    :return: modello kmeans ottimale
    """
    inertia_values = []
    k_values = list(range(k_min, k_max + 1))

    for k in k_values:
        kmed = KMedoids(
            n_clusters=k,
            metric="precomputed",
            random_state=random_state
        )
        kmed.fit(distance_matrix)
        inertia_values.append(kmed.inertia_)
    
    # Uso della libreria kneed per trovare il punto di "elbow"
    knee = KneeLocator(k_values, inertia_values, curve="convex", direction="decreasing")  
    print(f"Elbow found at k = {knee.knee}")

    best_kmedoids = KMedoids(n_clusters=knee.knee, metric="precomputed", random_state=random_state)
    best_kmedoids.fit(distance_matrix)
    
    return best_kmedoids

def compute_medoid(cluster_embeddings):
    """
    Calcola il medoid di un cluster dato un insieme di embeddings.

    :param cluster_embeddings: matrice degli embeddings del cluster
    :return: indice del medoid
    """
    distancematrix = pairwise_distances(cluster_embeddings, metric='cosine')
    total_distances = np.sum(distancematrix, axis=1)
  
    return np.argmin(total_distances)

def get_medoid_df(df, kmedoids):
    """
    Calcola i medoid globali per ogni cluster e crea un nuovo dataframe con le frequenze totali.
    
    :param df: dataframe delle varianti
    :param kmedoids: modello kmedoids applicato
    :return: dataframe con i medoid globali e le frequenze totali
    """
    medoid_indexes = kmedoids.medoid_indices_
    frequencies = []

    for cluster in range(kmedoids.n_clusters):
        indexes = np.where(kmedoids.labels_ == cluster)[0]
        frequencies.append(np.sum(df["frequency"].iloc[indexes]))

    df_medoid = df.iloc[medoid_indexes].copy()
    df_medoid["frequency"] = frequencies

    return df_medoid