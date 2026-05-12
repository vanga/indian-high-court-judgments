# from https://huggingface.co/spaces/Acetde/captchabreaker/tree/main
import numpy as np
import onnx
import onnxruntime as rt
from PIL import Image

model_file = "src/captcha_solver/captcha.onnx"
img_size = (32, 128)
charset = r"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"
itos = (["[E]"] + list(charset) + ["[UNK]", "[B]", "[P]"])
eos_id = 0


def preprocess_image(img_obj):
    # Match the old torchvision pipeline:
    # Resize -> ToTensor -> Normalize(0.5, 0.5)
    img = img_obj.convert("RGB").resize((img_size[1], img_size[0]), Image.BICUBIC)
    arr = np.asarray(img, dtype=np.float32) / 255.0
    arr = np.transpose(arr, (2, 0, 1))
    arr = (arr - 0.5) / 0.5
    return np.expand_dims(arr, axis=0)


def softmax(logits):
    shifted = logits - np.max(logits, axis=-1, keepdims=True)
    exp = np.exp(shifted)
    return exp / np.sum(exp, axis=-1, keepdims=True)


def decode_probs(probs):
    ids = probs.argmax(axis=-1).tolist()
    chars = []
    for token_id in ids:
        if token_id == eos_id:
            break
        if 0 <= token_id < len(itos):
            token = itos[token_id]
            if not token.startswith("["):
                chars.append(token)
    return "".join(chars)


def initialize_model(model_file):
    # Onnx model loading
    onnx_model = onnx.load(model_file)
    onnx.checker.check_model(onnx_model)
    ort_session = rt.InferenceSession(model_file)
    return ort_session


def get_text(img_path):
    img_obj = Image.open(img_path)
    # Preprocess. Model expects a batch of images with shape: (B, C, H, W)
    x = preprocess_image(img_obj)

    # compute ONNX Runtime output prediction
    ort_inputs = {ort_session.get_inputs()[0].name: x}
    logits = ort_session.run(None, ort_inputs)[0]
    probs = softmax(logits)
    return decode_probs(probs[0])


ort_session = initialize_model(model_file=model_file)


# if __name__ == "__main__":
#     image_path = "8000.png"
#     preds,probs = get_text(image_path)
#     print(preds[0])
