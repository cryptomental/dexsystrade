import os
import json
import string
from numpy import array
from numpy import argmax
from keras.models import Sequential
from keras.models import load_model
from keras.layers import Dense
from keras.layers import LSTM
from keras.layers import TimeDistributed
from keras.layers import RepeatVector


# integer encode strings
def integer_encode(x_input, y_input=None, encode_alphabet=None):
    if encode_alphabet is None:
        encode_alphabet = []
    char_to_int = dict((c, i) for i, c in enumerate(encode_alphabet))
    x_encoded = list()
    for pattern in x_input:
        integer_encoded = [char_to_int[char] for char in pattern if char in char_to_int]
        x_encoded.append(integer_encoded)
    if y_input:
        y_encoded = list()
        for pattern in y_input:
            integer_encoded = [char_to_int[char] for char in pattern if char in char_to_int]
            y_encoded.append(integer_encoded)
        return x_encoded, y_encoded
    else:
        return x_encoded


# one hot encode
def one_hot_encode(x_input, y_input=None, max_int=0):
    x_encoded = list()
    for seq in x_input:
        pattern = list()
        for index in seq:
            vector = [0 for _ in range(max_int)]
            vector[index] = 1
            pattern.append(vector)
        x_encoded.append(pattern)
    if y_input:
        y_encoded = list()
        for seq in y_input:
            pattern = list()
            for index in seq:
                vector = [0 for _ in range(max_int)]
                vector[index] = 1
                pattern.append(vector)
            y_encoded.append(pattern)
        return x_encoded, y_encoded
    else:
        return x_encoded


# generate an encoded data set
def generate_data(x_input, y_input, encode_alphabet):
    global longest_text_x
    global longest_text_y

    x_input = [next_str.rjust(longest_text_x, ' ') for next_str in x_input]
    y_input = [next_str.rjust(longest_text_y, ' ') for next_str in y_input]

    x_input, y_input = integer_encode(x_input, y_input, encode_alphabet)

    # one hot encode
    x_input, y_input = one_hot_encode(x_input, y_input, len(encode_alphabet))

    # return as numpy arrays
    x_input, y_input = array(x_input), array(y_input)
    return x_input, y_input


def generate_x(x_input, encode_alphabet):
    global longest_text_x

    x_input = [next_str.rjust(longest_text_x, ' ') for next_str in x_input]

    # integer encode
    x_input = integer_encode(x_input, encode_alphabet=encode_alphabet)
    # one hot encode
    x_input = one_hot_encode(x_input, max_int=len(encode_alphabet))
    
    # return as numpy arrays
    x_input = array(x_input)

    return x_input


# invert encoding
def invert(matrix_list, encode_alphabet):
    int_to_char_dict = dict((i, c) for i, c in enumerate(encode_alphabet))
    print int_to_char_dict
    strings = list()
    #print len(matrix_list)
    #print matrix_list[0]
    #print matrix_list[1]
    for i in range(len(matrix_list)):
        #print "matrix", i, matrix_list[i]
        #print "argmax", argmax(matrix_list[i])
        #matrix_list[i][106] = 0
        #matrix_list[i][26] = 0
        max_idx = 0
        max_max = matrix_list[i][0]
        for idx, a in enumerate(matrix_list[i]):
            if a > max_max:
                max_max = a
                max_idx = idx
        print max_idx, max_max
        char = int_to_char_dict[max_idx]
        # print pattern, char
        strings.append(char)
    return ''.join(strings)

# configure problem


# scope of possible symbols for each input or output time step
alphabet = [str(idx) for idx in range(10)] + ['+', ' '] + list(string.printable)

file_dir = os.path.dirname(__file__)   # <-- absolute dir the script is in

# fit LSTM
f = open(os.path.join(file_dir, "sentences.txt"))
x = f.readlines()
f.close()

f = open(os.path.join(file_dir, "sentences2.txt"))
y = f.readlines()
f.close() 

longest_text_x = len(max(x, key=len))
longest_text_y = len(max(y, key=len))

x, y = generate_data(x, y, alphabet)

# size of alphabet: (12 for 0-9, + and ' ')
n_chars = len(alphabet)
# length of encoded input sequence
n_in_seq_length = x.shape[1]  # n_terms * ceil(log10(largest+1)) + n_terms - 1
# length of encoded output sequence (2 for '30')
n_out_seq_length = y.shape[1]

# define LSTM
model = Sequential()
if not os.path.exists("model.h5"):
    model.add(LSTM(600, input_shape=(n_in_seq_length, n_chars)))
    model.add(RepeatVector(n_out_seq_length))
    model.add(LSTM(400, return_sequences=True))
    model.add(TimeDistributed(Dense(n_chars, activation='softmax')))
    model.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'])
    model.fit(x, y, epochs=10, batch_size=100)
    model.save("model.h5")
else:
    model = load_model("model.h5")

print(model.summary())

f = open(os.path.join(file_dir, "sentences.txt"))
x = f.readlines()
f.close()

f = open(os.path.join(file_dir, "sentences2.txt"))
y = f.readlines()
f.close()

# evaluate LSTM
x, y = generate_data(x, y, alphabet)

loss, acc = model.evaluate(x, y, verbose=0)
print('Loss: %f, Accuracy: %f' % (loss, acc*100))

f = open(os.path.join(file_dir, "sentences.txt"))
x = f.readlines()
f.close()

f = open(os.path.join(file_dir, "test_result.txt"), 'w')

# predict
for line in x:
    # make prediction
    x1 = generate_x([line], alphabet)
    y_predicted_inverted = model.predict(x1, verbose=1)
    print len(y_predicted_inverted[0])
    predicted = invert(y_predicted_inverted[0], alphabet)
    print "line", line, "predicted", predicted
    print "*" * 60
    f.write(predicted + os.linesep)

f.close()
