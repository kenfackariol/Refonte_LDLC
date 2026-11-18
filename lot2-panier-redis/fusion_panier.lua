-- Fusion du panier anonyme dans le panier du client à la connexion.
-- Exécuté côté serveur Redis : atomique, aucun autre client ne s'intercale.
--
-- KEYS[1] = panier anonyme    (ex : cart:anon:<sessionId>)
-- KEYS[2] = panier du client  (ex : cart:user:<userId>)
-- ARGV[1] = TTL du panier client en secondes
--
-- Usage : redis-cli --eval fusion_panier.lua cart:anon:s-abc cart:user:42 , 2592000

local items = redis.call('HGETALL', KEYS[1])
for i = 1, #items, 2 do
  -- on additionne les quantités si le produit est déjà dans le panier du client
  redis.call('HINCRBY', KEYS[2], items[i], tonumber(items[i + 1]))
end
redis.call('DEL', KEYS[1])
if redis.call('EXISTS', KEYS[2]) == 1 then
  redis.call('EXPIRE', KEYS[2], tonumber(ARGV[1]))
end
return #items / 2   -- nombre de lignes fusionnées
