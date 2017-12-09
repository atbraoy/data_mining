from math import exp
from math import exp

# loading from local model(s)
from error_backpropagate import backward_propagate_error


"""
A basic approach for a neural network model (standard)
"""

def neurons_activate(weights, inputs):
	"""
	A basic 'simple' model for neuron activation.
	Activation: sum(x_i*w_i)
	x_i: input signals into each neuron
	w_i: weight at each neuron
	[returns the activated neurons]
	"""
	activation = weights[-1] # w[-1] is the base neuron
	for i in range(len(weights)-1):
		activation += weights[i] * inputs[i]
		
	return activation


def neurons_transfer(activation):
	"""
	Neuron transfer is done through 'sigmoid function'. Othere functions can be used
	sigmoid = 1/(1e^(-a))
	a: activation
	[returns sigmoid]
	"""
	sigmoid = 1.0 / (1.0 + exp(-activation))
	
	return sigmoid


def forward_propagate(network, row):
	"""
	Take the neuron activations from the input layer
	Propagate activations through the network (i.e., going through the hidden layers)
	
	[returns inputs (neuron value)]
	"""
	inputs = row # rows are coming from the dataset entries
	for layer in network:
		new_inputs = []
		for neuron in layer:
			activation = neurons_activate(neuron['weights'], inputs)
			neuron['output'] = neurons_transfer(activation)
			new_inputs.append(neuron['output'])
		inputs = new_inputs
		
	return inputs


def weights_update(network, row, l_rate):
	"""
	Updating the weights at each layer with error measure through backpropagation
	"""
	for i in range(len(network)):
		inputs = row[:-1]
		if i != 0:
			inputs = [neuron['output'] for neuron in network[i - 1]]
		for neuron in network[i]:
			for j in range(len(inputs)):
				neuron['weights'][j] += l_rate * neuron['delta'] * inputs[j]
			neuron['weights'][-1] += l_rate * neuron['delta']


def train_neurons(network, train, l_rate, n_epoch, n_outputs):
	"""
	Train neurons cycling through the n epochs
	Back propagate the error
	Update the weights (after error propagation)
	"""
	for epoch in range(n_epoch):
		for row in train:
			outputs = forward_propagate(network, row)
			expected = [0 for i in range(n_outputs)]
			expected[row[-1]] = 1
			backward_propagate_error(network, expected)
			weights_update(network, row, l_rate)


def predict_neurons(network, row):
	"""
	Gives prediction for each neuron
	"""
	outputs = forward_propagate(network, row)
	prediction = outputs.index(max(outputs))
	
	return prediction
