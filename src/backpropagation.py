from random import random

# loading from local model(s)
from neural_network import train_neurons, predict_neurons

#----------------------------------------------------
def network_initializer(n_inputs, n_hidden, n_outputs):
	"""
	Initialize the network by firing input neuron's weights using random numbers
	[returns the initiated network]
	"""
	network = list()
	
	# declare the hidden layer
	hidden_layer = [{'weights':[random() for i in range(n_inputs + 1)]} for i in range(n_hidden)]
	network.append(hidden_layer)
	
	# declare the output layer 
	output_layer = [{'weights':[random() for i in range(n_hidden + 1)]} for i in range(n_outputs)]
	network.append(output_layer)
	
	return network


def back_propagation(train, test, l_rate, n_epoch, n_hidden):
	"""
	Train neurons cycling through the n epochs
	Back propagate the error
	Update the weights (after error propagation)
	Measure predictions
	[return predictions]
	"""
	n_inputs = len(train[0]) - 1
	n_outputs = len(set([row[-1] for row in train]))
	network = network_initializer(n_inputs, n_hidden, n_outputs)
	train_neurons(network, train, l_rate, n_epoch, n_outputs)
	predictions = list()
	for row in test:
		prediction = predict_neurons(network, row)
		predictions.append(prediction)
		
	return(predictions)
