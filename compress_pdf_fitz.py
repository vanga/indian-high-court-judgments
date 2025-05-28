import fitz  # PyMuPDF
import sys
import os

def compress_pdf(input_path, output_path, image_quality=40):
    doc = fitz.open(input_path)

    for page_index in range(len(doc)):
        page = doc.load_page(page_index)
        images = page.get_images(full=True)

        for img_index, img in enumerate(images):
            xref = img[0]
            base_image = doc.extract_image(xref)
            image_bytes = base_image["image"]

            # Recompress image
            pix = fitz.Pixmap(doc, xref)
            if pix.n > 4:  # convert CMYK to RGB
                pix = fitz.Pixmap(fitz.csRGB, pix)
            pix.save("temp_image.jpg", quality=image_quality)

            # Replace the image in the PDF
            new_xref = doc.insert_image(page.rect, filename="temp_image.jpg")
            doc._delete_object(xref)  # optional: delete original image

            os.remove("temp_image.jpg")

    doc.save(output_path, garbage=4, deflate=True)
    doc.close()
    print(f"Compressed PDF saved to: {output_path}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python compress_pdf_fitz.py input.pdf output.pdf")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    compress_pdf(input_file, output_file)
