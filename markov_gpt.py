import pyspark as sc
import random
import re



def choose_first_word():
    word = input("Podaj słowo od którego zacząć, jeśli chcesz przypadkowe wpisz X").lower()
    if word == "x":
        return random.choice(list(term_followers_dict.keys()))
    word_available = term_followers_dict.get(word, 0) != 0
    while not word_available:
        print("Podane słowo nie jest dostępne, podaj nowe")
        word = input("Podaj słowo od którego zacząć, jeśli chcesz przypadkowe wpisz X").lower()
        word_available = term_followers_dict.get(word, 0) != 0
    print(f"wybrano słowo {word}")
    return word

def prepare_text(line):
    line = line.lower()
    line = re.sub(r"[,.\"\'!@#$%^&*(){}?/;`~:<>+=-\\]", "", line)
    return line.split()

def generate_sentence(words, length=10):
    return ' '.join(random.choice(words) for _ in range(length))

def simulate_text_data(num_records, sentence_length=5, sentences_per_paragraph=5):
    for _ in range(num_records):
        paragraph = '. '.join(generate_sentence(words, sentence_length) for _ in range(sentences_per_paragraph))
        paragraph += '.'
        yield paragraph

# Plik tekstowy na RDD
text_file_rdd = sc.textFile('/FileStore/tables/pulp_fiction.txt')

# Obróbka tekstu
words_rdd = text_file_rdd.filter(lambda line: len(line) > 0).flatMap(prepare_text)

# przesunięcie rdd
shifted_rdd = words_rdd.zipWithIndex().map(lambda x: (x[1], x[0])).sortByKey().map(lambda x: x[1]).zipWithIndex().map(lambda x: (x[1] - 1, x[0])).filter(lambda x: x[0] >= 0)

# zzipowanie dwóch powstałych rdd
zipped_rdd = words_rdd.zipWithIndex().map(lambda x: (x[1], x[0])).join(shifted_rdd)

# mapowanie rdd
paired_rdd = zipped_rdd.map(lambda x: (x[1][0], x[1][1]))

# grupowanie i zebranie do całości
grouped_followers = paired_rdd.groupByKey().mapValues(list).collect()
print(grouped_followers)
term_followers_dict = {term: followers for term, followers in grouped_followers}
term_counts = {term: {follower: followers.count(follower) for follower in followers} for term, followers in term_followers_dict.items()}

# funkcja wdrażająca łańcuch markowa
def generate_next_term(current_term):
    followers = term_followers_dict.get(current_term, [])
    if followers:
        next_term = random.choices(followers, [term_counts[current_term][follower] for follower in followers])[0]
        return next_term
    else:
        return None

# Wybór pierwszego słowa
initial_term = choose_first_word()
current_term = initial_term
sentence_len = int(input("Podaj długość zdania"))
# Generowanie wybranej ilości słów przy pomocy łańcucha Markowa
generated_terms = [current_term]
for _ in range(sentence_len):
    next_term = generate_next_term(current_term)
    if next_term is not None:
        generated_terms.append(next_term)
        current_term = next_term
    else:
        break

# Display the generated terms
print(f"Starting from {initial_term}, Generated sentence: {' '.join(generated_terms)}")