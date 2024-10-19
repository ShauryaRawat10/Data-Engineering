## Machine Learning
Machine Learning algorithm is an algorithm that is able to learn from data

    Work with Huge data    ->                   Find patterns              -> Make intelligent decisions

    eg:  Emails on Server  ->                   Spam or Ham?                ->  Trash or Inbox

    eg2: Images represented as pixels   ->  Identify edges, colors and shapes -> A photo of a litle girl


#### Classification
Identifying which category or categories an observation belongs to

eg: Whales: Fish or Mammals?
    Mammals: Member of infraorder cetacea
    Fish: Look like fish, Swim like fish, and move with fish

> Rule based Binary classifier

    Whale   ->   Rule-based classifier  ->   Mammal
                    (Human experts)

    > Human expert to formulate rule
    > Rules specific to problem and data

> ML-Based Binary Classifier

    Breathes like mammal              ->        ML-based classifier     ->    Mammal
    Gives birth like a mammal                      (corpus)

    > Data used to train model parameters
    > Parameters of your model is trained by corpus of data in training phase


#### ML- based Classifier
1. Training: Feed in a large corpus of data classified correctly
2. Prediction: Use it to classify new instances which it has not seen before

> Training the ML-based Classifier

     Corpus   ->  ML-based classifier                ->                           Classification
                                        <---improves model parameter-----


