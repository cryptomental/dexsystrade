from random import seed
from random import randint
from numpy import array
from math import ceil
from math import log10
from math import sqrt
from numpy import argmax
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from keras.layers import TimeDistributed
from keras.layers import RepeatVector
import string

# integer encode strings
def integer_encode(X, y, alphabet):
	char_to_int = dict((c, i) for i, c in enumerate(alphabet))
	Xenc = list()
	for pattern in X:
		#print("X:")  
		#[print(char) for char in pattern]     
		integer_encoded = [char_to_int[char] for char in pattern if char in char_to_int ]
		Xenc.append(integer_encoded)
	yenc = list()
	for pattern in y:
		#print("y:")  
		#[print(char) for char in pattern]          
		integer_encoded = [char_to_int[char] for char in pattern if char in char_to_int ]
		yenc.append(integer_encoded)
	return Xenc, yenc

# integer encode strings
def integer_encode_X(X, alphabet):
	char_to_int = dict((c, i) for i, c in enumerate(alphabet))
	Xenc = list()
	for pattern in X:
		#print("X:")  
		#[print(char) for char in pattern]     
		integer_encoded = [char_to_int[char] for char in pattern if char in char_to_int ]
		Xenc.append(integer_encoded)

	return Xenc

# one hot encode
def one_hot_encode(X, y, max_int):
	Xenc = list()
	for seq in X:
		pattern = list()
		for index in seq:
			vector = [0 for _ in range(max_int)]
			vector[index] = 1
			pattern.append(vector)
		Xenc.append(pattern)
	yenc = list()
	for seq in y:
		pattern = list()
		for index in seq:
			vector = [0 for _ in range(max_int)]
			vector[index] = 1
			pattern.append(vector)
		yenc.append(pattern)
	return Xenc, yenc

# one hot encode
def one_hot_encode_X(X, max_int):
	Xenc = list()
	for seq in X:
		pattern = list()
		for index in seq:
			vector = [0 for _ in range(max_int)]
			vector[index] = 1
			pattern.append(vector)
		Xenc.append(pattern)
	return Xenc

longesttext = 0
longesttexty = 0
# generate an encoded dataset
def generate_data(X,y,alphabet):
    
	global longesttext
	global longesttexty   
    
	X = [nextstr.rjust(longesttext,' ') for nextstr in X]
    
	y = [nextstr.rjust(longesttexty,' ') for nextstr in y]
    
	#print(str(X))
    
	# integer encode
	X, y = integer_encode(X, y, alphabet)
	# one hot encode
	X, y = one_hot_encode(X, y, len(alphabet))
    
	# return as numpy arrays
	X, y = array(X), array(y)
	return X, y

def generate_X(X,alphabet):
    
	global longesttext 
    
	X = [nextstr.rjust(longesttext,' ') for nextstr in X]
    
	# integer encode
	X  = integer_encode_X(X, alphabet)
	# one hot encode
	X  = one_hot_encode_X(X, len(alphabet))
    
	# return as numpy arrays
	X = array(X)
    
	return X

# invert encoding
def invert(seq, alphabet):
	int_to_char = dict((i, c) for i, c in enumerate(alphabet))
	strings = list()
	for pattern in seq:
		string = int_to_char[argmax(pattern)]
		strings.append(string)
	return ''.join(strings)

# configure problem

# scope of possible symbols for each input or output time step
alphabet = [str(x) for x in range(10)] + ['+', ' '] + list(string.printable)

import os 
file_dir = os.path.dirname(__file__) #<-- absolute dir the script is in

# fit LSTM
f = open(os.path.join(file_dir, "sentences.txt"))
X = f.readlines()
f.close()

f = open(os.path.join(file_dir, "sentences2.txt"))
y= f.readlines()
f.close() 
    
longesttext = len(max(X, key=len))
longesttexty = len(max(y, key=len))
    
X, y = generate_data(X,y,alphabet)

# size of alphabet: (12 for 0-9, + and ' ')
n_chars = len(alphabet)
# length of encoded input sequence
n_in_seq_length = X.shape[1]#n_terms * ceil(log10(largest+1)) + n_terms - 1
# length of encoded output sequence (2 for '30')
n_out_seq_length = y.shape[1]

# define LSTM
model = Sequential()
model.add(LSTM(600, input_shape=(n_in_seq_length, n_chars)))
model.add(RepeatVector(n_out_seq_length))
model.add(LSTM(400, return_sequences=True))
model.add(TimeDistributed(Dense(n_chars, activation='softmax')))
model.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'])
print(model.summary())

model.fit(X, y, epochs=2, batch_size=1000)


f = open(os.path.join(file_dir, "sentences.txt"))
X = f.readlines()
f.close()

f = open(os.path.join(file_dir, "sentences2.txt"))
y= f.readlines()
f.close()

# evaluate LSTM
X, y = generate_data(X,y,alphabet)

loss, acc = model.evaluate(X, y, verbose=0)
print('Loss: %f, Accuracy: %f' % (loss, acc*100))

f = open(os.path.join(file_dir, "sentences.txt"))
X = f.readlines()
f.close()
    
f = open(os.path.join(file_dir, "testresult.txt"), 'a')
# predict
for x in X:
	# make prediction
	x1 = list()
	x1.append(x)
	x1 = generate_X(x1,alphabet) 
	yhat = model.predict(x1, verbose=0)
	predicted = invert(yhat[0], alphabet)
	f.write(predicted + "\n")   

f.close() 
