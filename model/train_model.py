import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score

#placeholder data
data = pd.read_csv('data/processed/training_data.csv')
x = data.drop(columns=['label'])
y = data['label']

X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=0.2)
clf = RandomForestClassifier()
clf.fit(X_train, y_train)

preds = clf.predict(X_test)
print("Accuracy:", accuracy_score(y_test, preds))

