# Desafio B2W

## Objetivo
Desenvolver um job para encontrar carrinhos abandonados pelos clientes de um e-commerce. 

## Solução
Através da API do Apache Beam para Python, foi desenvolvido um job que, a partir de eventos em formato JSON persistidos localmente, aplica determinados critérios que caracterizam abandono de carrinho e persiste localmente eventos que se enquadram nesses critérios.  

## Pre requisitos
* Python 3.7

## Instalação
    git clone https://github.com/ynfialho/de_challenge_b2w.git
    cd de_challenge_b2w

    python3 -m pip install -r requirements.txt

## Como executar
    python3 process_abandoned_carts.py

## Teste
    python3 -m unittest discover
