from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

password = input("Ingresa la contraseña para generar el hash: ")
hash_value = pwd_context.hash(password)

print("\nHash generado:")
print(hash_value)
