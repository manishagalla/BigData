from nltk.corpus import wordnet as wn
from nltk import word_tokenize
from nltk.stem import WordNetLemmatizer

def lesk(word,sentence):
    synsets = wn.synsets(word)
    max_overlap=0
    best_sense= synsets[0]
    wl = WordNetLemmatizer()
    context = word_tokenize(sentence)
    context = [wl.lemmatize(i) for i in context]
    #print context
    for sense in range(len(synsets)):
        tokens = []
        df=synsets[sense]._definition
        ex=synsets[sense]._examples
        for i in range(len(ex)):
            tokens += word_tokenize(ex[i])
        tokens += word_tokenize(df)
        signature = [wl.lemmatize(i) for i in tokens]
        #print "signature: ", signature
        #print synsets[sense], synsets[sense]._definition
        overlap=0
        for tkn in context:
            for each in signature:
                if tkn==each:
                    #print tkn
                    overlap +=1
        #print "overlap count: ",overlap
        if overlap > max_overlap:
            max_overlap = overlap
            best_sense=synsets[sense]
    return best_sense

sentence = input("enter sentence:")
word = input("enter ambiguous word:")
b_sense = lesk(word,sentence)
print "The best sense for the word ",word," is ", repr(b_sense)
print "The definition of the synset is: ", b_sense._definition


