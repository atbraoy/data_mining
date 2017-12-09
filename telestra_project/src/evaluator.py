from random import randrange

#-----------------------------------
def fold_splitter(dataset, n_folds):
	"""
	Typical splitting procedure, split the dataset into several n folds
	[returns a lsit of splitted n folds of data]
	"""
	dataset_split = list()
	dataset_clone = list(dataset)
	fold_size = int(len(dataset) / n_folds)
	for i in range(n_folds):
		fold = list()
		while len(fold) < fold_size:
			index = randrange(len(dataset_clone))
			fold.append(dataset_clone.pop(index)) # remove the indexed value and append it to the 'clone'
		dataset_split.append(fold)
		
	return dataset_split


def accuracy_measure(original, predicted):
	"""
	Find the percentage/ratio of prediction deviation from the given value/classification
	[returns 'original/predicted' ratio]
	"""
	correct = 0
	for i in range(len(original)):
		if original[i] == predicted[i]:
			correct += 1
	ratio = correct / float(len(original)) * 100.0
	
	return ratio


def score_measure(dataset, model, n_folds, *args):
	"""
	Produce accuracy score for each fold (splitted dataset)
	[return scores]
	"""
	folds = fold_splitter(dataset, n_folds)
	scores = list()
	rank = 0 # initiate fold count
	for fold in folds:
		train_set = list(folds)
		train_set.remove(fold) # remove one fold each time
		train_set = sum(train_set, [])
		test_set = list()
		for row in fold:
			row_clone = list(row)
			test_set.append(row_clone)
			row_clone[-1] = None
		predicted = model(train_set, test_set, *args)
		actual = [row[-1] for row in fold]
		accuracy = accuracy_measure(actual, predicted)
		print 'accuracy measured: {0}, at fold: {1}'.format(accuracy, rank) 
		scores.append(accuracy) # score measure at each fold
		rank+= 1
		
	return scores
# -----------------------------