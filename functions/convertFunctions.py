class convertFunctions:
    def __init__(self):
        pass

    def isbn13_to_isbn10(isbn13):
        # Remover o hífen, se houver
        isbn13 = isbn13.replace("-", "").replace(" ", "")
        # Verificar se o ISBN-13 é válido
        if len(isbn13) != 13 or not isbn13.isdigit() or (isbn13[:3] not in ["978", "979"]):
            print(isbn13)
            raise ValueError("ISBN-13 inválido")

        # Manter os últimos 10 dígitos
        isbn10_candidate = isbn13[3:]

        # Calcular o dígito de controle do ISBN-10
        total = sum(int(isbn10_candidate[i]) * (10 - i) for i in range(9))
        check_digit = total % 11

        # Formatar o dígito de controle
        if check_digit == 10:
            check_digit_str = 'X'
        else:
            check_digit_str = str(check_digit)

        # Retornar o ISBN-10
        return isbn10_candidate + check_digit_str

    def isbn10_to_isbn13(isbn10):
        # Remover o hífen, se houver
        isbn10 = isbn10.replace("-", "").replace(" ", "")

        # Verificar se o ISBN-10 é válido
        if len(isbn10) != 10 or not isbn10[:-1].isdigit() or (isbn10[-1] not in "0123456789X"):
            print(isbn10)
            raise ValueError("ISBN-10 inválido")

        # Retirar o dígito de controle
        isbn9 = isbn10[:-1]

        # Adicionar prefixo 978
        isbn13 = "978" + isbn9

        # Calcular o dígito de controle do ISBN-13
        total = sum(int(isbn13[i]) * (1 if i % 2 == 0 else 3) for i in range(12))
        check_digit = (10 - (total % 10)) % 10

        # Retornar o ISBN-13
        return isbn13 + str(check_digit)