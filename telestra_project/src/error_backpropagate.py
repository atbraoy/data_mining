#---------------------------------
def transfer_derivative(output):
	"""
	Calculate the derivative of an neuron output:
	Derivative: d/da(1/(1+e^(-activation)))
	[returns derivative]
	"""
	derivative = output * (1.0 - output)
	
	return derivative


def backward_propagate_error(network, expected):
	"""
	Backpropagate error and store it in neurons
	"""
	for i in reversed(range(len(network))):
		layer = network[i]
		errors = list()
		if i != len(network)-1:
			for j in range(len(layer)):
				error = 0.0
				for neuron in network[i + 1]:
					error += (neuron['weights'][j] * neuron['delta'])
				errors.append(error)
		else:
			for j in range(len(layer)):
				neuron = layer[j]
				errors.append(expected[j] - neuron['output'])
		for j in range(len(layer)):
			neuron = layer[j]
			neuron['delta'] = errors[j] * transfer_derivative(neuron['output'])
			