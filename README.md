# SistemaBanco

## Criar ambiente virtual

```
python -m venv .venv
```

## Habilitar ambiente virtual

```
.venv\Scripts\activate
```

## Upgrade pip

```
python.exe -m pip install --upgrade pip
```

## Instalar dependências

```
pip install -r requirements.txt
```

## Rodar banco de dados (inicialmente, vamos alterar depois)

```
cd SistemaBanco\Bancodedados\

python criar_tabelas.py
python popular_tabelas.py
python visualizar_dados.py
```

### Extensão SQLite

`SQLite Viewer`
