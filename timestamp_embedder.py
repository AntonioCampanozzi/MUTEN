from keras.models import Sequential
from keras.layers import LSTM
from keras.layers import Dense
from keras.layers import RepeatVector
from keras.layers import TimeDistributed
from keras.layers import Masking
from keras.preprocessing.sequence import pad_sequences
from keras.src import Input
import tensorflow as tf

PADDING=-42.0
LSTM_LAYERS=256
DENSE_LAYERS=128
EPOCHS=30
PATIENCE=3
BATCH_SIZE=32


def masked_mse(y_true, y_pred):
    # mask = 1 dove NON è padding, 0 dove c'è padding
    mask = tf.cast(tf.not_equal(y_true, PADDING), tf.float32)

    # errore quadratico
    se = tf.square(y_true - y_pred) * mask

    # media solo sui timestep validi
    return tf.reduce_sum(se) / tf.reduce_sum(mask)

class TimeEmbedder:
    def __init__(self, max_sequence_length):
        self.max_sequence_length=max_sequence_length
        self.model = Sequential()
        self.model.add(Input(shape=(max_sequence_length,1)))
        self.model.add(Masking(mask_value=PADDING))
        self.model.add(LSTM(LSTM_LAYERS, activation='relu'))
        self.model.add(Dense(DENSE_LAYERS, activation='relu'))
        self.model.add(RepeatVector(max_sequence_length))
        self.model.add(Dense(DENSE_LAYERS, activation='relu'))
        self.model.add(LSTM(LSTM_LAYERS, activation='relu', return_sequences=True))
        self.model.add(TimeDistributed(Dense(1)))
        self.model.compile(optimizer='adam', loss=masked_mse)
    
    def input_mask(self, sequences):
        return pad_sequences(sequences,
                             maxlen=self.max_sequence_length,
                             dtype='float32',
                             padding='post',
                             value=PADDING)
    
    def fit(self, sequences):
        callback = tf.keras.callbacks.EarlyStopping(
                monitor='loss',
                patience=PATIENCE,
                restore_best_weights=True
            )
        
        self.model.fit(
            sequences, sequences,
            epochs=EPOCHS,
            batch_size=BATCH_SIZE,
            callbacks=[callback],
            shuffle=True
        )
        
        return self.model
    
    def get_embeddings(self, sequences):
        encoder = Model(
        inputs=self.model.input,
        outputs=self.model.layers[3].output  # Dense(128)
        )
        return encoder.predict(sequences)
    
    