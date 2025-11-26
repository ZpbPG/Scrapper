import torch

data = torch.load("course_embeddings.pt")

print(data.keys())
emb = data["embeddings"]
course_map = data["course_map"]

print("SHAPE")
print(emb.shape)          # rozmiar tensora
print("course_map")
print(course_map[:3])     # pierwsze 3 wpisy mapy kursów
print("emb")
print(emb[0][:10])        # pierwsze 10 liczb pierwszego embeddingu